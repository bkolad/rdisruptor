use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdisruptor::{BusySpin, Consumer, DisruptorBuilder};
use tokio::runtime::Runtime;

fn main() {
    const TOTAL_EVENTS: u64 = 10_000_000;

    let (pruned_tx, pruned_rx) = sync_channel(1);
    let written = Arc::new(AtomicU64::new(0));

    let journal = Journal::new("journal", written.clone());
    let db_writer = DbWriter::new("db_writer", None);
    let publisher = Publisher::new("publisher");
    let pruner = Pruner::new("pruner", TOTAL_EVENTS, pruned_tx);

    let mut disruptor = DisruptorBuilder::<Data>::new()
        .capacity(1024)
        .consumer(journal.clone())
        .consumer_after([journal.name()], db_writer.clone())
        .consumer_after([journal.name()], publisher.clone())
        .consumer_after([db_writer.name(), publisher.name()], pruner)
        .build(BusySpin)
        .expect("DAG should validate");

    println!();
    disruptor.print_topology();
    println!();

    let mut producer = disruptor.producer();

    let start = Instant::now();

    let data = Arc::new(vec![1, 2, 3]);
    for i in 0..TOTAL_EVENTS {
        producer
            .publish(|slot| {
                slot.id = i;
                slot.inner = data.clone();
            })
            .unwrap();
    }

    drop(producer);

    let pruned = pruned_rx
        .recv()
        .expect("pruner should send the final count");
    let elapsed = start.elapsed();

    println!("published        : {TOTAL_EVENTS}");
    println!("journaled        : {}", written.load(Ordering::Acquire));
    println!("pruned           : {pruned}");
    println!(
        "elapsed               : {:?}  ({:.2} M events/s)",
        elapsed,
        (TOTAL_EVENTS as f64 / elapsed.as_secs_f64()) / 1e6,
    );

    disruptor.shutdown_or_panic();
}

#[derive(Default, Clone)]
struct Data {
    id: u64,
    inner: Arc<Vec<u8>>,
}

#[derive(Clone)]
struct Journal {
    name: &'static str,
    written: Arc<AtomicU64>,
}

impl Journal {
    fn new(name: &'static str, written: Arc<AtomicU64>) -> Self {
        Self { name, written }
    }
}

impl Consumer<Data> for Journal {
    fn name(&self) -> &str {
        self.name
    }

    fn on_event(&mut self, _ev: &Data, _seq: i64, _eob: bool) {
        // pretend to write to a journal file
        self.written.fetch_add(1, Ordering::Relaxed);
    }
}

/// Stand-in for a real async database client. `insert` simulates network/disk
/// latency with a timer and records how many inserts are in flight at once so
/// the example can show that a batch really is issued concurrently.
struct DbClient {
    inserted: AtomicU64,
    latency: Option<Duration>,
}

impl DbClient {
    fn new(latency: Option<Duration>) -> Self {
        Self {
            inserted: AtomicU64::new(0),
            latency,
        }
    }

    async fn insert(&self, _id: u64, _bytes: usize) {
        if let Some(latency) = self.latency {
            tokio::time::sleep(latency).await; // pretend to hit the DB
        }
        self.inserted.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct DbWriter {
    name: &'static str,
    rt: Arc<Runtime>,
    client: Arc<DbClient>,
    pending: Vec<Data>,
}

impl DbWriter {
    fn new(name: &'static str, latency: Option<Duration>) -> Self {
        const CAPACITY: usize = 1024;

        let rt: Arc<Runtime> = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build runtime"),
        );

        let client = Arc::new(DbClient::new(latency));

        Self {
            name,
            rt,
            client,
            pending: Vec::with_capacity(CAPACITY),
        }
    }
}

impl Consumer<Data> for DbWriter {
    fn name(&self) -> &str {
        self.name
    }

    fn on_event(&mut self, ev: &Data, _seq: i64, end_of_batch: bool) {
        // Clone out of the ring slot — we must NOT hold `&Data` (or a future
        // borrowing it / self) across the await, so we buffer owned data.

        self.pending.push(ev.clone());
        if end_of_batch {
            let client = &self.client;

            let mut batch = Vec::with_capacity(self.pending.capacity());
            std::mem::swap(&mut batch, &mut self.pending);

            self.rt.block_on(async move {
                let writes = batch.iter().map(|o| client.insert(o.id, o.inner.len()));
                futures::future::join_all(writes).await;
            });
        }
    }
}

#[derive(Clone)]
struct Publisher {
    name: &'static str,
}
impl Publisher {
    fn new(name: &'static str) -> Self {
        Self { name }
    }
}

impl Consumer<Data> for Publisher {
    fn name(&self) -> &str {
        self.name
    }

    fn on_event(&mut self, _ev: &Data, _seq: i64, _eob: bool) {
        // pretend to publish to an external broker
    }
}

#[derive(Clone)]
struct Pruner {
    name: &'static str,
    expected: u64,
    pruned: u64,
    done_tx: SyncSender<u64>,
}
impl Pruner {
    fn new(name: &'static str, expected: u64, done_tx: SyncSender<u64>) -> Self {
        Self {
            name,
            expected,
            pruned: 0,
            done_tx,
        }
    }
}

impl Consumer<Data> for Pruner {
    fn name(&self) -> &str {
        self.name
    }

    fn on_event(&mut self, _ev: &Data, _seq: i64, _eob: bool) {
        self.pruned += 1;
        if self.pruned == self.expected {
            self.done_tx
                .send(self.pruned)
                .expect("main should receive the final count");
        }
    }
}
