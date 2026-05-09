use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use rdisruptor::{
    spsc, BuildError, BusySpin, Consumer, DisruptorBuilder, PublishError, WaitStrategy,
};

struct Collector {
    out: Arc<Mutex<Vec<u64>>>,
    batches: Arc<AtomicUsize>,
    done_tx: mpsc::SyncSender<()>,
    expected: usize,
}

impl Consumer<u64> for Collector {
    fn on_event(&mut self, ev: &u64, _seq: i64, end_of_batch: bool) {
        let mut g = self.out.lock().unwrap();
        g.push(*ev);
        if end_of_batch {
            self.batches.fetch_add(1, Ordering::Relaxed);
        }
        if g.len() == self.expected {
            self.done_tx.send(()).unwrap();
        }
    }
}

#[test]
fn round_trip_in_order() {
    let out = Arc::new(Mutex::new(Vec::<u64>::new()));
    let batches = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::sync_channel(1);
    let consumer = Collector {
        out: Arc::clone(&out),
        batches: Arc::clone(&batches),
        done_tx: tx,
        expected: 100,
    };

    let mut disruptor = spsc::<u64, _, _>(16, BusySpin, consumer).unwrap();
    let mut producer = disruptor.producer();
    for i in 0..100u64 {
        producer.publish(|slot| *slot = i).unwrap();
    }
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    disruptor.shutdown_or_panic();

    let g = out.lock().unwrap();
    assert_eq!(g.len(), 100);
    for (i, v) in g.iter().enumerate() {
        assert_eq!(*v as usize, i, "out of order at {i}: {v}");
    }
}

struct BatchCollector {
    batch_ends: Arc<Mutex<Vec<i64>>>,
    ready_tx: mpsc::SyncSender<()>,
    release_rx: mpsc::Receiver<()>,
    done_tx: mpsc::SyncSender<()>,
    expected: i64,
}

impl Consumer<u64> for BatchCollector {
    fn on_start(&mut self) {
        self.ready_tx.send(()).unwrap();
        self.release_rx.recv().unwrap();
    }

    fn on_event(&mut self, _ev: &u64, seq: i64, end_of_batch: bool) {
        if end_of_batch {
            self.batch_ends.lock().unwrap().push(seq);
        }
        if seq == self.expected - 1 {
            self.done_tx.send(()).unwrap();
        }
    }
}

#[test]
fn max_batch_size_caps_batches_and_closes_partial_batch() {
    let batch_ends = Arc::new(Mutex::new(Vec::new()));
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let consumer = BatchCollector {
        batch_ends: Arc::clone(&batch_ends),
        ready_tx,
        release_rx,
        done_tx,
        expected: 8,
    };

    let mut disruptor = DisruptorBuilder::<u64>::new()
        .capacity(16)
        .max_batch_size(3)
        .consumer(consumer)
        .build(BusySpin)
        .unwrap();
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut producer = disruptor.producer();
    for i in 0..8u64 {
        producer.publish(|slot| *slot = i).unwrap();
    }
    release_tx.send(()).unwrap();

    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    disruptor.shutdown_or_panic();

    assert_eq!(*batch_ends.lock().unwrap(), vec![2, 5, 7]);
}

#[test]
fn wraparound_many_passes() {
    // capacity 4, send 4096 events => 1024 wraparounds
    let n: usize = 4096;
    let out = Arc::new(Mutex::new(Vec::<u64>::new()));
    let batches = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::sync_channel(1);
    let consumer = Collector {
        out: Arc::clone(&out),
        batches,
        done_tx: tx,
        expected: n,
    };

    let mut disruptor = spsc::<u64, _, _>(4, BusySpin, consumer).unwrap();
    let mut producer = disruptor.producer();
    for i in 0..n as u64 {
        producer.publish(|slot| *slot = i).unwrap();
    }
    rx.recv_timeout(Duration::from_secs(10)).unwrap();
    disruptor.shutdown_or_panic();

    let g = out.lock().unwrap();
    assert_eq!(g.len(), n);
    for (i, v) in g.iter().enumerate() {
        assert_eq!(*v as usize, i);
    }
}

#[test]
fn backpressure_blocks_producer_until_drain() {
    // Slow consumer: fixed sleep per event. Producer must block.
    struct Slow {
        seen: usize,
        expected: usize,
        done_tx: mpsc::SyncSender<()>,
    }
    impl Consumer<u64> for Slow {
        fn on_event(&mut self, _ev: &u64, _seq: i64, _eob: bool) {
            std::thread::sleep(Duration::from_millis(10));
            self.seen += 1;
            if self.seen == self.expected {
                self.done_tx.send(()).unwrap();
            }
        }
    }

    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let consumer = Slow {
        seen: 0,
        expected: 8,
        done_tx,
    };
    let mut disruptor = spsc::<u64, _, _>(4, BusySpin, consumer).unwrap();
    let mut producer = disruptor.producer();

    let start = Instant::now();
    for i in 0..8u64 {
        producer.publish(|slot| *slot = i).unwrap();
    }
    // Producer must have spent some time waiting on the slow consumer.
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(30),
        "producer returned too fast: {:?}",
        elapsed
    );

    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    disruptor.shutdown_or_panic();
}

struct Noop;
impl Consumer<u64> for Noop {
    fn on_event(&mut self, _ev: &u64, _seq: i64, _eob: bool) {}
}

#[test]
fn spsc_invalid_capacity_errors() {
    assert!(matches!(
        spsc::<u64, _, _>(3, BusySpin, Noop),
        Err(BuildError::InvalidCapacity { capacity: 3 })
    ));
}

#[test]
fn shutdown_cleanly_with_no_events() {
    let mut disruptor = spsc::<u64, _, _>(8, BusySpin, Noop).unwrap();
    let producer = disruptor.producer();
    drop(producer);
    disruptor.shutdown_or_panic();
}

struct BlockingBatchConsumer {
    ready_tx: mpsc::SyncSender<()>,
    start_rx: mpsc::Receiver<()>,
    first_event_tx: mpsc::SyncSender<()>,
    release_rx: mpsc::Receiver<()>,
    seen: Arc<Mutex<Vec<i64>>>,
}

impl Consumer<u64> for BlockingBatchConsumer {
    fn on_start(&mut self) {
        self.ready_tx.send(()).unwrap();
        self.start_rx.recv().unwrap();
    }

    fn on_event(&mut self, _ev: &u64, sequence: i64, _end_of_batch: bool) {
        self.seen.lock().unwrap().push(sequence);
        if sequence == 0 {
            self.first_event_tx.send(()).unwrap();
            self.release_rx.recv().unwrap();
        }
    }
}

#[test]
fn shutdown_stops_between_events_in_an_acquired_batch() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (start_tx, start_rx) = mpsc::sync_channel(1);
    let (first_event_tx, first_event_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let consumer = BlockingBatchConsumer {
        ready_tx,
        start_rx,
        first_event_tx,
        release_rx,
        seen: Arc::clone(&seen),
    };

    let mut disruptor = spsc::<u64, _, _>(8, BusySpin, consumer).unwrap();
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut producer = disruptor.producer();
    for value in 0..8 {
        producer.publish(|slot| *slot = value).unwrap();
    }

    start_tx.send(()).unwrap();
    first_event_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let shutdown = std::thread::spawn(move || disruptor.shutdown());

    // This publication waits for either consumer progress or the shutdown
    // alert. Its failure proves the alert is visible before event 0 returns.
    assert_eq!(
        producer.publish(|slot| *slot = 8),
        Err(PublishError::Shutdown)
    );
    release_tx.send(()).unwrap();

    shutdown.join().unwrap().unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![0]);
}

#[test]
fn publish_after_shutdown_returns_shutdown() {
    let mut disruptor = spsc::<u64, _, _>(8, BusySpin, Noop).unwrap();
    let mut producer = disruptor.producer();

    disruptor.shutdown_or_panic();

    assert_eq!(
        producer.publish(|slot| *slot = 42),
        Err(PublishError::Shutdown)
    );
}

struct EagerIdle;

impl WaitStrategy for EagerIdle {
    fn idle(&self, _attempt: u32) {}
}

struct PublicationProbe {
    started_tx: mpsc::SyncSender<()>,
    event_tx: mpsc::SyncSender<i64>,
}

impl Consumer<u64> for PublicationProbe {
    fn on_start(&mut self) {
        self.started_tx.send(()).unwrap();
    }

    fn on_event(&mut self, _ev: &u64, sequence: i64, _end_of_batch: bool) {
        self.event_tx.send(sequence).unwrap();
    }
}

#[test]
fn wait_strategy_cannot_expose_unpublished_events() {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (event_tx, event_rx) = mpsc::sync_channel(1);
    let consumer = PublicationProbe {
        started_tx,
        event_tx,
    };

    let mut disruptor = spsc::<u64, _, _>(8, EagerIdle, consumer).unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        event_rx.recv_timeout(Duration::from_millis(20)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );

    let mut producer = disruptor.producer();
    producer.publish(|slot| *slot = 42).unwrap();
    assert_eq!(event_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 0);
    disruptor.shutdown_or_panic();
}

// --- drop counting -----------------------------------------------------------

static CREATED: AtomicUsize = AtomicUsize::new(0);
static DROPPED: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
#[allow(dead_code)]
struct Counted(u64);

impl Counted {
    fn new(v: u64) -> Self {
        CREATED.fetch_add(1, Ordering::SeqCst);
        Counted(v)
    }
}

impl Default for Counted {
    fn default() -> Self {
        CREATED.fetch_add(1, Ordering::SeqCst);
        Counted(0)
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        DROPPED.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn drops_are_balanced() {
    CREATED.store(0, Ordering::SeqCst);
    DROPPED.store(0, Ordering::SeqCst);

    struct Sink {
        count: Arc<AtomicUsize>,
        expected: usize,
        done_tx: mpsc::SyncSender<()>,
    }
    impl Consumer<Counted> for Sink {
        fn on_event(&mut self, _ev: &Counted, _seq: i64, _eob: bool) {
            let n = self.count.fetch_add(1, Ordering::Release) + 1;
            if n == self.expected {
                self.done_tx.send(()).unwrap();
            }
        }
    }

    let count = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::sync_channel(1);
    let sink = Sink {
        count: Arc::clone(&count),
        expected: 100,
        done_tx: tx,
    };

    {
        let mut disruptor = spsc::<Counted, _, _>(8, BusySpin, sink).unwrap();
        let mut producer = disruptor.producer();
        for i in 0..100u64 {
            producer
                .publish(|slot| {
                    *slot = Counted::new(i);
                })
                .unwrap();
        }
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        disruptor.shutdown_or_panic();
        // ring + producer dropped here
    }

    let created = CREATED.load(Ordering::SeqCst);
    let dropped = DROPPED.load(Ordering::SeqCst);
    assert_eq!(
        created, dropped,
        "leaked Counted values: created={created} dropped={dropped}"
    );
}
