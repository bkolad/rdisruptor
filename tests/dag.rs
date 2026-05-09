//! End-to-end DAG: every consumer sees every event in order, and a downstream
//! consumer never sees an event before its upstream dependencies do.

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use rdisruptor::{BusySpin, Consumer, DisruptorBuilder, PublishError, Yielding};

/// Records each (sequence, downstream_observed_upstream_cursor) pair so we
/// can assert ordering after the run.
#[derive(Clone)]
struct Recording {
    name: String,
    upstream: Vec<Arc<AtomicI64>>,
    cursor: Arc<AtomicI64>,
    out: Arc<Mutex<Vec<(String, i64)>>>,
    expected: usize,
    seen: usize,
    done_tx: Option<mpsc::Sender<()>>,
}

impl Recording {
    fn new(
        name: &'static str,
        upstream: Vec<Arc<AtomicI64>>,
        cursor: Arc<AtomicI64>,
        out: Arc<Mutex<Vec<(String, i64)>>>,
        expected: usize,
        done_tx: Option<mpsc::Sender<()>>,
    ) -> Self {
        Self {
            name: name.into(),
            upstream,
            cursor,
            out,
            expected,
            seen: 0,
            done_tx,
        }
    }
}

impl Consumer<u64> for Recording {
    fn name(&self) -> &str {
        &self.name
    }

    fn on_event(&mut self, _ev: &u64, seq: i64, _eob: bool) {
        // Verify all upstream cursors are >= seq before we observe it.
        for up in &self.upstream {
            assert!(
                up.load(Ordering::Acquire) >= seq,
                "{}: upstream behind at seq {seq}",
                self.name
            );
        }
        self.out.lock().unwrap().push((self.name.clone(), seq));
        self.cursor.store(seq, Ordering::Release);
        self.seen += 1;
        if self.seen == self.expected {
            if let Some(tx) = self.done_tx.take() {
                let _ = tx.send(());
            }
        }
    }
}

#[test]
fn diamond_dag_respects_dependencies() {
    let n: usize = 10_000;
    let out = Arc::new(Mutex::new(Vec::<(String, i64)>::new()));

    let a_cursor = Arc::new(AtomicI64::new(-1));
    let b_cursor = Arc::new(AtomicI64::new(-1));
    let c_cursor = Arc::new(AtomicI64::new(-1));
    let d_cursor = Arc::new(AtomicI64::new(-1));

    let (tx_d, rx_d) = mpsc::channel();

    let a = Recording::new(
        "a",
        vec![],
        Arc::clone(&a_cursor),
        Arc::clone(&out),
        n,
        None,
    );
    let b = Recording::new(
        "b",
        vec![],
        Arc::clone(&b_cursor),
        Arc::clone(&out),
        n,
        None,
    );
    let c = Recording::new(
        "c",
        vec![Arc::clone(&a_cursor), Arc::clone(&b_cursor)],
        Arc::clone(&c_cursor),
        Arc::clone(&out),
        n,
        None,
    );
    let d = Recording::new(
        "d",
        vec![Arc::clone(&c_cursor)],
        Arc::clone(&d_cursor),
        Arc::clone(&out),
        n,
        Some(tx_d),
    );
    let expected_names = [
        a.name().to_string(),
        b.name().to_string(),
        c.name().to_string(),
        d.name().to_string(),
    ];

    let mut disruptor = DisruptorBuilder::<u64>::new()
        .capacity(64)
        .consumer(a.clone())
        .consumer(b.clone())
        .consumer_after([a.name(), b.name()], c.clone())
        .consumer_after([c.name()], d)
        .build(BusySpin)
        .unwrap();

    let mut producer = disruptor.producer();
    for i in 0..n as u64 {
        producer.publish(|slot| *slot = i).unwrap();
    }
    rx_d.recv_timeout(Duration::from_secs(10))
        .expect("d did not finish");
    disruptor.shutdown_or_panic();

    let g = out.lock().unwrap();
    assert_eq!(
        g.len(),
        n * 4,
        "expected each of 4 consumers to see {n} events"
    );

    // Each consumer should see seqs 0..n in order.
    let mut per: std::collections::HashMap<String, Vec<i64>> = std::collections::HashMap::new();
    for (name, seq) in g.iter() {
        per.entry(name.clone()).or_default().push(*seq);
    }
    for name in expected_names {
        let seqs = per.get(&name).expect(&name);
        assert_eq!(seqs.len(), n);
        for (i, s) in seqs.iter().enumerate() {
            assert_eq!(*s as usize, i, "{name} seq mismatch at {i}");
        }
    }
}

#[derive(Clone)]
struct Counter {
    name: &'static str,
    c: Arc<AtomicUsize>,
    expected: usize,
    done: Option<mpsc::Sender<()>>,
}
impl Counter {
    fn new(
        name: &'static str,
        c: Arc<AtomicUsize>,
        expected: usize,
        done: Option<mpsc::Sender<()>>,
    ) -> Self {
        Self {
            name,
            c,
            expected,
            done,
        }
    }
}
impl Consumer<u64> for Counter {
    fn name(&self) -> &str {
        self.name
    }

    fn on_event(&mut self, _ev: &u64, _seq: i64, _eob: bool) {
        let n = self.c.fetch_add(1, Ordering::Release) + 1;
        if n == self.expected {
            if let Some(tx) = self.done.take() {
                let _ = tx.send(());
            }
        }
    }
}

/// Exercise the Yielding wait strategy on the DAG path so it doesn't bit-rot.
#[test]
fn yielding_strategy_works_on_dag() {
    let c1 = Arc::new(AtomicUsize::new(0));
    let c2 = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel();
    let first = Counter::new("first", Arc::clone(&c1), 1000, None);
    let second = Counter::new("second", Arc::clone(&c2), 1000, Some(tx));

    let mut disruptor = DisruptorBuilder::<u64>::new()
        .capacity(16)
        .consumer(first.clone())
        .consumer_after([first.name()], second)
        .build(Yielding::new())
        .unwrap();

    let mut producer = disruptor.producer();
    for i in 0..1000u64 {
        producer.publish(|slot| *slot = i).unwrap();
    }
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    disruptor.shutdown_or_panic();
    assert_eq!(c1.load(Ordering::Acquire), 1000);
    assert_eq!(c2.load(Ordering::Acquire), 1000);
}

struct PanickingConsumer;

impl Consumer<u64> for PanickingConsumer {
    fn name(&self) -> &str {
        "panicker"
    }

    fn on_event(&mut self, _ev: &u64, _sequence: i64, _end_of_batch: bool) {
        panic!("intentional consumer failure");
    }
}

struct ShutdownNotifier {
    tx: mpsc::SyncSender<()>,
}

impl Consumer<u64> for ShutdownNotifier {
    fn name(&self) -> &str {
        "peer"
    }

    fn on_event(&mut self, _ev: &u64, _sequence: i64, _end_of_batch: bool) {}

    fn on_shutdown(&mut self) {
        self.tx.send(()).unwrap();
    }
}

#[test]
fn consumer_panic_alerts_producer_and_is_reported_by_shutdown() {
    let (shutdown_tx, shutdown_rx) = mpsc::sync_channel(1);
    let mut disruptor = DisruptorBuilder::<u64>::new()
        .capacity(8)
        .consumer(PanickingConsumer)
        .consumer(ShutdownNotifier { tx: shutdown_tx })
        .build(BusySpin)
        .unwrap();

    let mut producer = disruptor.producer();
    producer.publish(|slot| *slot = 1).unwrap();

    // The peer's shutdown callback proves the panicking worker set the shared
    // alert and released the rest of the consumer graph.
    shutdown_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("peer consumer did not stop after another consumer panicked");
    assert_eq!(
        producer.publish(|slot| *slot = 2),
        Err(PublishError::Shutdown)
    );

    let error = disruptor
        .shutdown()
        .expect_err("shutdown should report the panicked consumer");
    assert_eq!(error.panicked_consumers(), &[String::from("panicker")]);
}
