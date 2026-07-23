//! AFTER: pipeline whose events carry an inline reusable payload buffer.
//! No shared refcount line; steady-state publishes allocate nothing.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Arc;
use std::time::Instant;

use rdisruptor::{BusySpin, Consumer, DisruptorBuilder};
use tokio::runtime::Runtime;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

const TOTAL_EVENTS: u64 = 10_000_000;

#[derive(Default)]
struct Data {
    id: u64,
    payload: Vec<u8>,
}

struct Journal;

impl Consumer<Data> for Journal {
    fn name(&self) -> &str {
        "journal"
    }

    fn on_event(&mut self, _ev: &Data, _seq: i64, _eob: bool) {}
}

struct DbClient {
    inserted: AtomicU64,
}

impl DbClient {
    async fn insert(&self, _id: u64, _bytes: usize) {
        self.inserted.fetch_add(1, Ordering::Relaxed);
    }
}

struct DbWriter {
    rt: Runtime,
    client: Arc<DbClient>,
    pending: Vec<(u64, usize)>,
}

impl Consumer<Data> for DbWriter {
    fn name(&self) -> &str {
        "db_writer"
    }

    fn on_event(&mut self, ev: &Data, _seq: i64, end_of_batch: bool) {
        self.pending.push((ev.id, ev.payload.len()));
        if end_of_batch {
            let client = &self.client;
            let mut batch = Vec::with_capacity(self.pending.capacity());
            std::mem::swap(&mut batch, &mut self.pending);
            self.rt.block_on(async move {
                let writes = batch.iter().map(|&(id, bytes)| client.insert(id, bytes));
                futures::future::join_all(writes).await;
            });
        }
    }
}

struct Publisher;

impl Consumer<Data> for Publisher {
    fn name(&self) -> &str {
        "publisher"
    }

    fn on_event(&mut self, _ev: &Data, _seq: i64, _eob: bool) {}
}

struct Pruner {
    pruned: u64,
    done_tx: SyncSender<u64>,
}

impl Consumer<Data> for Pruner {
    fn name(&self) -> &str {
        "pruner"
    }

    fn on_event(&mut self, _ev: &Data, _seq: i64, _eob: bool) {
        self.pruned += 1;
        if self.pruned == TOTAL_EVENTS {
            self.done_tx.send(self.pruned).unwrap();
        }
    }
}

fn main() {
    let (done_tx, done_rx) = sync_channel(1);

    let db_writer = DbWriter {
        rt: tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
        client: Arc::new(DbClient {
            inserted: AtomicU64::new(0),
        }),
        pending: Vec::with_capacity(1024),
    };

    let mut disruptor = DisruptorBuilder::<Data>::new()
        .capacity(1024)
        .consumer(Journal)
        .consumer_after(["journal"], db_writer)
        .consumer_after(["journal"], Publisher)
        .consumer_after(["db_writer", "publisher"], Pruner { pruned: 0, done_tx })
        .build(BusySpin)
        .unwrap();

    let mut producer = disruptor.producer();

    let allocs_before = ALLOCS.load(Ordering::Relaxed);
    let start = Instant::now();

    let payload = [1u8, 2, 3];
    for i in 0..TOTAL_EVENTS {
        producer
            .publish(|slot| {
                slot.id = i;
                slot.payload.clear();
                slot.payload.extend_from_slice(&payload);
            })
            .unwrap();
    }

    let pruned = done_rx.recv().unwrap();
    let elapsed = start.elapsed();
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;

    println!("variant      : pipline AFTER (inline reusable payload)");
    println!("events       : {TOTAL_EVENTS} (pruned {pruned})");
    println!("elapsed      : {elapsed:?}");
    println!(
        "throughput   : {:.2} M events/s",
        (TOTAL_EVENTS as f64 / elapsed.as_secs_f64()) / 1e6
    );
    println!("allocations  : {allocs}");
    println!(
        "allocs/event : {:.3}",
        allocs as f64 / TOTAL_EVENTS as f64
    );

    disruptor.shutdown_or_panic();
}
