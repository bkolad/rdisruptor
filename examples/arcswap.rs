//! Shows how a consumer can update data inside a published ring-buffer event.
//! `ArcSwap` provides interior mutability, while the consumer dependency makes
//! sure the reader observes the updater's replacement.

use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use rdisruptor::{BusySpin, Consumer, DisruptorBuilder};

const EVENT_COUNT: u64 = 12;

fn main() {
    let (done_tx, done_rx) = sync_channel(1);

    let updater = ArcSwapUpdater;
    let reader = ArcSwapReader {
        values: Vec::with_capacity(EVENT_COUNT as usize),
        done_tx,
    };

    let mut disruptor = DisruptorBuilder::<Event>::new()
        .capacity(8)
        .max_batch_size(3)
        .consumer(updater)
        // The dependency guarantees that the updater has published its
        // cursor before the reader observes the corresponding event.
        .consumer_after([updater.name()], reader)
        .build(BusySpin)
        .expect("consumer graph should be valid");

    disruptor.print_topology();

    let mut producer = disruptor.producer();
    for input in 0..EVENT_COUNT {
        producer
            .publish(|event| {
                event.input = input;
                // Ring slots are reused, so clear the previous occupant's
                // result before publishing this event.
                event.shared.store(Arc::new(0));
            })
            .expect("disruptor should still be running");
    }
    drop(producer);

    let values = done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("reader should consume every event before the timeout");
    println!("values read from ArcSwap: {values:?}");

    disruptor.shutdown_or_panic();
}

struct Event {
    input: u64,
    shared: ArcSwap<u64>,
}

impl Default for Event {
    fn default() -> Self {
        Self {
            input: 0,
            shared: ArcSwap::from_pointee(0),
        }
    }
}

#[derive(Clone, Copy)]
struct ArcSwapUpdater;

impl Consumer<Event> for ArcSwapUpdater {
    fn name(&self) -> &str {
        "arc_swap_updater"
    }

    fn on_event(&mut self, event: &Event, _sequence: i64, _end_of_batch: bool) {
        event.shared.store(Arc::new(event.input + 100));
    }
}

struct ArcSwapReader {
    values: Vec<u64>,
    done_tx: SyncSender<Vec<u64>>,
}

impl Consumer<Event> for ArcSwapReader {
    fn name(&self) -> &str {
        "arc_swap_reader"
    }

    fn on_event(&mut self, event: &Event, sequence: i64, _end_of_batch: bool) {
        let value = *event.shared.load_full();
        assert_eq!(value, event.input + 100);
        self.values.push(value);

        if sequence == EVENT_COUNT as i64 - 1 {
            let values = std::mem::take(&mut self.values);
            self.done_tx
                .send(values)
                .expect("main thread should receive the values");
        }
    }
}
