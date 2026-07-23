//! A stage that writes its result INTO the published ring-buffer event with a
//! plain store — no interior mutability, no per-event allocation, no atomics
//! inside the handler.
//!
//! Registering the stage with `consumer_mut` makes `build()` prove, from the
//! DAG alone, that it holds exclusive access to each slot while it runs:
//! every other stage must be an ancestor or a descendant, so no thread can
//! observe the slot mid-mutation. The downstream reader is gated on the
//! annotator's cursor and therefore sees the completed mutation.

use std::sync::mpsc::{sync_channel, SyncSender};
use std::time::Duration;

use rdisruptor::{BusySpin, Consumer, DisruptorBuilder, MutConsumer};

const EVENT_COUNT: u64 = 12;

fn main() {
    let (done_tx, done_rx) = sync_channel(1);

    let reader = Reader {
        values: Vec::with_capacity(EVENT_COUNT as usize),
        done_tx,
    };

    let mut disruptor = DisruptorBuilder::<Event>::new()
        .capacity(8)
        .max_batch_size(3)
        .consumer_mut(Annotator)
        // The dependency guarantees the annotator's cursor — and therefore
        // its in-place mutation — is visible before the reader runs.
        .consumer_after(["annotator"], reader)
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
                event.result = 0;
            })
            .expect("disruptor should still be running");
    }
    drop(producer);

    let values = done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("reader should consume every event before the timeout");
    println!("annotated values: {values:?}");

    disruptor.shutdown_or_panic();
}

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

    fn on_event(&mut self, event: &mut Event, _sequence: i64, _end_of_batch: bool) {
        event.result = event.input + 100;
    }
}

struct Reader {
    values: Vec<u64>,
    done_tx: SyncSender<Vec<u64>>,
}

impl Consumer<Event> for Reader {
    fn name(&self) -> &str {
        "reader"
    }

    fn on_event(&mut self, event: &Event, sequence: i64, _end_of_batch: bool) {
        assert_eq!(event.result, event.input + 100);
        self.values.push(event.result);

        if sequence == EVENT_COUNT as i64 - 1 {
            let values = std::mem::take(&mut self.values);
            self.done_tx
                .send(values)
                .expect("main thread should receive the values");
        }
    }
}
