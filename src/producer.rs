use std::sync::Arc;

use crate::ring::RingBuffer;
use crate::sequence::{Sequence, INITIAL};
use crate::sync::{AtomicBool, Ordering};
use crate::wait::WaitStrategy;

#[derive(Debug, PartialEq, Eq)]
pub enum PublishError {
    /// The disruptor has been alerted by shutdown or a consumer failure.
    Shutdown,
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shutdown => write!(f, "disruptor is shut down"),
        }
    }
}

impl std::error::Error for PublishError {}

pub struct SingleProducer<T, W: WaitStrategy> {
    ring: Arc<RingBuffer<T>>,
    cursor: Arc<Sequence>,
    gating: Vec<Arc<Sequence>>,
    alert: Arc<AtomicBool>,
    wait: Arc<W>,
    next_seq: i64,
    cached_gate: i64,
    capacity: i64,
}

impl<T, W: WaitStrategy> SingleProducer<T, W> {
    pub(crate) fn new(
        ring: Arc<RingBuffer<T>>,
        cursor: Arc<Sequence>,
        gating: Vec<Arc<Sequence>>,
        alert: Arc<AtomicBool>,
        wait: Arc<W>,
    ) -> Self {
        let capacity = ring.capacity() as i64;
        Self {
            ring,
            cursor,
            gating,
            alert,
            wait,
            next_seq: 0,
            cached_gate: INITIAL,
            capacity,
        }
    }

    fn min_gate(&self) -> i64 {
        let mut m = i64::MAX;
        for g in &self.gating {
            let v = g.get();
            if v < m {
                m = v;
            }
        }
        m
    }
}

impl<T: Send + Sync, W: WaitStrategy> SingleProducer<T, W> {
    /// Publish one event.
    ///
    /// Returns [`PublishError::Shutdown`] when the disruptor was already
    /// alerted as this call began. A publish already in progress may overlap
    /// with a concurrent shutdown.
    pub fn publish<F: FnOnce(&mut T)>(&mut self, write: F) -> Result<i64, PublishError> {
        if self.alert.load(Ordering::Acquire) {
            return Err(PublishError::Shutdown);
        }

        let seq = self.next_seq;
        let wrap_point = seq - self.capacity;

        if self.cached_gate < wrap_point {
            let mut min = self.min_gate();
            let mut attempt = 0u32;
            while min < wrap_point {
                if self.alert.load(Ordering::Acquire) {
                    return Err(PublishError::Shutdown);
                }
                self.wait.idle(attempt);
                attempt = attempt.saturating_add(1);
                min = self.min_gate();
            }
            self.cached_gate = min;
        }

        // SAFETY: producer holds exclusive access to slot[seq & mask] until
        // cursor.set(seq) below. No consumer reads it until cursor >= seq.
        // No producer overwrites it because next call uses seq + 1, and the
        // wrap_point check guarantees no consumer is still reading the
        // previous occupant at this index.
        let slot = unsafe { &mut *self.ring.slot_ptr(seq) };
        write(slot);
        self.cursor.set(seq);

        self.next_seq = seq + 1;
        Ok(seq)
    }
}

#[cfg(test)]
mod tests {
    use super::PublishError;

    #[test]
    fn publish_error_is_a_standard_error() {
        fn assert_error<E: std::error::Error>() {}

        assert_error::<PublishError>();
        assert_eq!(PublishError::Shutdown.to_string(), "disruptor is shut down");
    }
}
