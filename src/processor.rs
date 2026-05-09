use std::sync::Arc;

use crate::barrier::SequenceBarrier;
use crate::consumer::BoxedConsumer;
use crate::ring::RingBuffer;
use crate::sequence::Sequence;
use crate::wait::{WaitResult, WaitStrategy};

pub(crate) struct EventProcessor<T, W: WaitStrategy> {
    consumer: BoxedConsumer<T>,
    cursor: Arc<Sequence>,
    barrier: SequenceBarrier<W>,
    ring: Arc<RingBuffer<T>>,
    max_batch_size: i64,
}

impl<T, W> EventProcessor<T, W>
where
    T: Send + Sync + 'static,
    W: WaitStrategy,
{
    pub(crate) fn new(
        consumer: BoxedConsumer<T>,
        cursor: Arc<Sequence>,
        barrier: SequenceBarrier<W>,
        ring: Arc<RingBuffer<T>>,
        max_batch_size: i64,
    ) -> Self {
        Self {
            consumer,
            cursor,
            barrier,
            ring,
            max_batch_size,
        }
    }

    pub(crate) fn run(mut self) {
        self.consumer.on_start();
        let mut next = 0i64;
        'processing: while let WaitResult::Available(avail) = self.barrier.wait_for(next) {
            let batch_end = avail.min(next.saturating_add(self.max_batch_size - 1));
            let mut seq = next;
            while seq <= batch_end {
                if self.barrier.is_alerted() {
                    // Preserve the cursor at the last event actually delivered;
                    // the rest of the acquired batch has not been processed.
                    self.cursor.set(seq - 1);
                    break 'processing;
                }

                // SAFETY: producer published values up to `avail` before
                // the corresponding cursor.set(); our wait_for did an
                // Acquire load on that cursor, so all writes to slots
                // [next..=avail] happen-before this read. The producer
                // cannot overwrite these slots until our cursor passes
                // them (gating).
                let event: &T = unsafe { &*self.ring.slot_ptr(seq) };
                let end_of_batch = seq == batch_end;
                self.consumer.on_event(event, seq, end_of_batch);
                seq += 1;
            }
            self.cursor.set(batch_end);
            next = batch_end + 1;
        }
        self.consumer.on_shutdown();
    }
}
