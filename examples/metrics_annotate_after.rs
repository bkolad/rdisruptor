//! AFTER: the MutConsumer approach — the stage writes its result into the
//! slot with a plain store. Instrumented with a counting allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::time::Instant;

use rdisruptor::{BusySpin, Consumer, DisruptorBuilder, MutConsumer};

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

const EVENTS: u64 = 10_000_000;

#[derive(Default)]
struct Event {
    input: u64,
    result: u64,
}

struct Annotator;

impl MutConsumer<Event> for Annotator {
    fn name(&self) -> &str {
        "annotator"
    }

    fn on_event(&mut self, event: &mut Event, _seq: i64, _eob: bool) {
        event.result = event.input + 100;
    }
}

struct Reader {
    sum: u64,
    done_tx: SyncSender<u64>,
}

impl Consumer<Event> for Reader {
    fn name(&self) -> &str {
        "reader"
    }

    fn on_event(&mut self, event: &Event, seq: i64, _eob: bool) {
        self.sum = self.sum.wrapping_add(event.result);
        if seq == EVENTS as i64 - 1 {
            self.done_tx.send(self.sum).unwrap();
        }
    }
}

fn main() {
    let (done_tx, done_rx) = sync_channel(1);

    let mut disruptor = DisruptorBuilder::<Event>::new()
        .capacity(1024)
        .consumer_mut(Annotator)
        .consumer_after(["annotator"], Reader { sum: 0, done_tx })
        .build(BusySpin)
        .unwrap();

    let mut producer = disruptor.producer();

    let allocs_before = ALLOCS.load(Ordering::Relaxed);
    let start = Instant::now();

    for input in 0..EVENTS {
        producer
            .publish(|event| {
                event.input = input;
                event.result = 0;
            })
            .unwrap();
    }

    let sum = done_rx.recv().unwrap();
    let elapsed = start.elapsed();
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;

    println!("variant      : annotate AFTER (MutConsumer)");
    println!("events       : {EVENTS}");
    println!("checksum     : {sum}");
    println!("elapsed      : {elapsed:?}");
    println!(
        "throughput   : {:.2} M events/s",
        (EVENTS as f64 / elapsed.as_secs_f64()) / 1e6
    );
    println!("allocations  : {allocs}");
    println!(
        "allocs/event : {:.3}",
        allocs as f64 / EVENTS as f64
    );

    disruptor.shutdown_or_panic();
}
