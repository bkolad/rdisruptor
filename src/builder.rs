use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::barrier::SequenceBarrier;
use crate::consumer::{BoxedConsumer, Consumer};
use crate::processor::EventProcessor;
use crate::producer::SingleProducer;
use crate::ring::RingBuffer;
use crate::sequence::Sequence;
use crate::sync::{AtomicBool, Ordering};
use crate::wait::WaitStrategy;

#[derive(Debug, PartialEq, Eq)]
pub enum BuildError {
    MissingCapacity,
    InvalidCapacity {
        capacity: usize,
    },
    InvalidMaxBatchSize,
    EmptyDag,
    InvalidConsumerName {
        consumer: String,
    },
    DuplicateName(String),
    UnknownDependency {
        consumer: String,
        dep: String,
    },
    Cycle,
    SpawnFailed {
        consumer: String,
        kind: std::io::ErrorKind,
    },
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCapacity => write!(f, "capacity must be set before build()"),
            Self::InvalidCapacity { capacity } => write!(
                f,
                "capacity must be a power of two, at least 2, and fit in i64; got {capacity}"
            ),
            Self::InvalidMaxBatchSize => write!(f, "max batch size must be at least 1"),
            Self::EmptyDag => write!(f, "no consumers were registered"),
            Self::InvalidConsumerName { consumer } => write!(
                f,
                "invalid consumer name {consumer:?}: names may not contain NUL bytes"
            ),
            Self::DuplicateName(n) => write!(f, "duplicate consumer name: {n}"),
            Self::UnknownDependency { consumer, dep } => {
                write!(f, "consumer '{consumer}' depends on unknown '{dep}'")
            }
            Self::Cycle => write!(f, "consumer dependency graph contains a cycle"),
            Self::SpawnFailed { consumer, kind } => {
                write!(f, "failed to spawn consumer '{consumer}' thread: {kind}")
            }
        }
    }
}

impl std::error::Error for BuildError {}

/// Error returned by [`Disruptor::shutdown`] when consumer threads panicked.
#[derive(Debug, PartialEq, Eq)]
pub struct ShutdownError {
    panicked_consumers: Vec<String>,
}

impl ShutdownError {
    /// Names of all consumers whose threads panicked.
    pub fn panicked_consumers(&self) -> &[String] {
        &self.panicked_consumers
    }
}

impl std::fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "consumer threads panicked: {}",
            self.panicked_consumers.join(", ")
        )
    }
}

impl std::error::Error for ShutdownError {}

struct ConsumerSpec<T> {
    name: String,
    deps: Vec<String>,
    consumer: BoxedConsumer<T>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TopologyNode {
    name: String,
    deps: Vec<String>,
}

struct ConsumerHandle {
    name: String,
    handle: JoinHandle<()>,
}

fn join_workers(handles: &mut Vec<ConsumerHandle>) -> Vec<String> {
    let mut panicked_consumers = Vec::new();
    for worker in handles.drain(..) {
        if worker.handle.join().is_err() {
            panicked_consumers.push(worker.name);
        }
    }
    panicked_consumers
}

pub struct DisruptorBuilder<T> {
    capacity: Option<usize>,
    max_batch_size: Option<usize>,
    nodes: Vec<ConsumerSpec<T>>,
}

impl<T> Default for DisruptorBuilder<T> {
    fn default() -> Self {
        Self {
            capacity: None,
            max_batch_size: None,
            nodes: Vec::new(),
        }
    }
}

impl<T: Default + Send + Sync + 'static> DisruptorBuilder<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn capacity(mut self, cap: usize) -> Self {
        self.capacity = Some(cap);
        self
    }

    /// Limit the number of events delivered to a consumer in one batch.
    ///
    /// The limit applies independently to every consumer. By default it is
    /// the ring capacity, preserving the behavior of draining all currently
    /// available events as one batch.
    pub fn max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = Some(max_batch_size);
        self
    }

    /// Register a consumer that depends only on the producer cursor.
    pub fn consumer<C>(mut self, consumer: C) -> Self
    where
        C: Consumer<T> + 'static,
    {
        let name = consumer.name().to_string();
        self.nodes.push(ConsumerSpec {
            name,
            deps: Vec::new(),
            consumer: Box::new(consumer),
        });
        self
    }

    /// Register a consumer that depends on all named upstream consumers.
    pub fn consumer_after<C, D>(mut self, deps: D, consumer: C) -> Self
    where
        C: Consumer<T> + 'static,
        D: IntoIterator,
        D::Item: AsRef<str>,
    {
        let name = consumer.name().to_string();
        self.nodes.push(ConsumerSpec {
            name,
            deps: deps.into_iter().map(|s| s.as_ref().to_string()).collect(),
            consumer: Box::new(consumer),
        });
        self
    }

    pub fn build<W: WaitStrategy>(self, wait: W) -> Result<Disruptor<T, W>, BuildError> {
        let capacity = self.capacity.ok_or(BuildError::MissingCapacity)?;
        if capacity < 2 || !capacity.is_power_of_two() || capacity > i64::MAX as usize {
            return Err(BuildError::InvalidCapacity { capacity });
        }
        if self.max_batch_size == Some(0) {
            return Err(BuildError::InvalidMaxBatchSize);
        }
        if self.nodes.is_empty() {
            return Err(BuildError::EmptyDag);
        }

        let n = self.nodes.len();

        // 1. Unique names
        let mut name_to_idx: HashMap<String, usize> = HashMap::with_capacity(n);
        for (i, spec) in self.nodes.iter().enumerate() {
            if spec.name.contains('\0') {
                return Err(BuildError::InvalidConsumerName {
                    consumer: spec.name.clone(),
                });
            }
            if name_to_idx.insert(spec.name.clone(), i).is_some() {
                return Err(BuildError::DuplicateName(spec.name.clone()));
            }
        }

        // 2. Known deps
        for spec in &self.nodes {
            for d in &spec.deps {
                if !name_to_idx.contains_key(d) {
                    return Err(BuildError::UnknownDependency {
                        consumer: spec.name.clone(),
                        dep: d.clone(),
                    });
                }
            }
        }

        // 3. Cycle detection — Kahn's
        let mut in_degree = vec![0usize; n];
        let mut edges: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, spec) in self.nodes.iter().enumerate() {
            in_degree[i] = spec.deps.len();
            for d in &spec.deps {
                edges[name_to_idx[d]].push(i);
            }
        }
        let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
        let mut topological_order = Vec::with_capacity(n);
        while let Some(i) = queue.pop_front() {
            topological_order.push(i);
            for &j in &edges[i] {
                in_degree[j] -= 1;
                if in_degree[j] == 0 {
                    queue.push_back(j);
                }
            }
        }
        if topological_order.len() != n {
            return Err(BuildError::Cycle);
        }

        let topology = topological_order
            .iter()
            .map(|&i| TopologyNode {
                name: self.nodes[i].name.clone(),
                deps: self.nodes[i].deps.clone(),
            })
            .collect();

        // 4. Allocate cursors and decide leaves
        let cursors: Vec<Arc<Sequence>> = (0..n).map(|_| Arc::new(Sequence::default())).collect();

        let mut is_referenced = vec![false; n];
        for spec in &self.nodes {
            for d in &spec.deps {
                is_referenced[name_to_idx[d]] = true;
            }
        }
        let leaves: Vec<Arc<Sequence>> = (0..n)
            .filter(|&i| !is_referenced[i])
            .map(|i| Arc::clone(&cursors[i]))
            .collect();

        // 5. Build runtime
        let ring = Arc::new(RingBuffer::<T>::new(capacity));
        // RingBuffer::new has verified that capacity fits in i64. A larger
        // configured batch cannot be observed because backpressure limits the
        // available window to the ring capacity.
        let max_batch_size = self.max_batch_size.unwrap_or(capacity).min(capacity) as i64;
        let producer_cursor = Arc::new(Sequence::default());
        let alert = Arc::new(AtomicBool::new(false));
        let wait = Arc::new(wait);

        let mut handles = Vec::with_capacity(n);
        for (i, spec) in self.nodes.into_iter().enumerate() {
            let dep_seqs: Vec<Arc<Sequence>> = if spec.deps.is_empty() {
                vec![Arc::clone(&producer_cursor)]
            } else {
                spec.deps
                    .iter()
                    .map(|d| Arc::clone(&cursors[name_to_idx[d]]))
                    .collect()
            };
            let barrier = SequenceBarrier::new(dep_seqs, Arc::clone(&wait), Arc::clone(&alert));
            let processor = EventProcessor::new(
                spec.consumer,
                Arc::clone(&cursors[i]),
                barrier,
                Arc::clone(&ring),
                max_batch_size,
            );
            let consumer_name = spec.name;
            let worker_alert = Arc::clone(&alert);
            let worker_wait = Arc::clone(&wait);
            let spawn_result = thread::Builder::new()
                .name(format!("rdisruptor-{consumer_name}"))
                .spawn(move || {
                    let result = catch_unwind(AssertUnwindSafe(move || processor.run()));
                    if let Err(payload) = result {
                        worker_alert.store(true, Ordering::Release);
                        worker_wait.signal_all();
                        resume_unwind(payload);
                    }
                });

            match spawn_result {
                Ok(handle) => handles.push(ConsumerHandle {
                    name: consumer_name,
                    handle,
                }),
                Err(error) => {
                    // Release and join workers that were already started so
                    // they do not remain detached with the ring alive.
                    alert.store(true, Ordering::Release);
                    wait.signal_all();
                    let _ = join_workers(&mut handles);
                    return Err(BuildError::SpawnFailed {
                        consumer: consumer_name,
                        kind: error.kind(),
                    });
                }
            }
        }

        let producer = SingleProducer::new(
            Arc::clone(&ring),
            producer_cursor,
            leaves,
            Arc::clone(&alert),
            Arc::clone(&wait),
        );

        Ok(Disruptor {
            producer: Some(producer),
            handles,
            alert,
            wait,
            topology,
        })
    }
}

pub struct Disruptor<T, W: WaitStrategy> {
    producer: Option<SingleProducer<T, W>>,
    handles: Vec<ConsumerHandle>,
    alert: Arc<AtomicBool>,
    wait: Arc<W>,
    topology: Vec<TopologyNode>,
}

impl<T: Send + Sync + 'static, W: WaitStrategy> Disruptor<T, W> {
    /// Take the producer. Panics if called twice.
    pub fn producer(&mut self) -> SingleProducer<T, W> {
        self.producer.take().expect("producer already taken")
    }

    /// Print the consumer dependency graph in topological order.
    pub fn print_topology(&self) {
        print!("{}", self.format_topology());
    }

    fn format_topology(&self) -> String {
        let mut out = String::from("Disruptor topology:\n");
        // Char count, not byte length: the formatter measures `{:<width$}`
        // padding in chars, so a byte-length width over-pads multi-byte names.
        let name_width = self
            .topology
            .iter()
            .map(|node| node.name.chars().count())
            .max()
            .unwrap_or(0);

        for node in &self.topology {
            let deps = if node.deps.is_empty() {
                "producer".to_string()
            } else {
                node.deps.join(", ")
            };
            let _ = writeln!(out, "  {:<width$} <- {deps}", node.name, width = name_width);
        }
        out
    }

    /// Signal all consumers to stop and join them, reporting any consumer
    /// threads that panicked.
    ///
    /// This is an immediate stop and does not drain pending events. After this
    /// method returns, new calls to [`SingleProducer::publish`] return
    /// [`PublishError::Shutdown`](crate::PublishError::Shutdown). A publish
    /// already in progress concurrently with shutdown may still complete;
    /// coordinate or drop the producer first when a strict cutoff is required.
    ///
    /// The alert only interrupts consumers that are idling or between events;
    /// it cannot preempt user code. A consumer blocked inside
    /// [`Consumer::on_event`](crate::Consumer::on_event) will not observe the
    /// alert until it returns, so this call joins — and therefore blocks —
    /// until every consumer's current event completes.
    pub fn shutdown(mut self) -> Result<(), ShutdownError> {
        self.alert.store(true, Ordering::Release);
        self.wait.signal_all();
        let panicked_consumers = join_workers(&mut self.handles);

        if panicked_consumers.is_empty() {
            Ok(())
        } else {
            Err(ShutdownError { panicked_consumers })
        }
    }

    /// Signal all consumers to stop and join them, panicking if any consumer
    /// thread panicked.
    ///
    /// Publication cutoff semantics are the same as [`Self::shutdown`].
    #[track_caller]
    pub fn shutdown_or_panic(self) {
        if let Err(error) = self.shutdown() {
            panic!("{error}");
        }
    }
}

impl<T, W: WaitStrategy> Drop for Disruptor<T, W> {
    fn drop(&mut self) {
        if !self.handles.is_empty() {
            self.alert.store(true, Ordering::Release);
            self.wait.signal_all();
            let _ = join_workers(&mut self.handles);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wait::BusySpin;

    #[derive(Clone, Copy)]
    struct Noop {
        name: &'static str,
    }

    impl Noop {
        fn new(name: &'static str) -> Self {
            Self { name }
        }
    }

    impl Consumer<u64> for Noop {
        fn name(&self) -> &str {
            self.name
        }

        fn on_event(&mut self, _event: &u64, _sequence: i64, _end_of_batch: bool) {}
    }

    #[test]
    fn topology_is_printed_in_dependency_order() {
        let journal = Noop::new("journal");
        let db_writer = Noop::new("db_writer");
        let publisher = Noop::new("publisher");
        let pruner = Noop::new("pruner");

        let disruptor = DisruptorBuilder::<u64>::new()
            .capacity(16)
            .consumer(journal)
            .consumer_after([journal.name()], db_writer)
            .consumer_after([journal.name()], publisher)
            .consumer_after([db_writer.name(), publisher.name()], pruner)
            .build(BusySpin)
            .unwrap();

        assert_eq!(
            disruptor.format_topology(),
            "Disruptor topology:\n  journal   <- producer\n  db_writer <- journal\n  publisher <- journal\n  pruner    <- db_writer, publisher\n"
        );

        disruptor.shutdown_or_panic();
    }
}
