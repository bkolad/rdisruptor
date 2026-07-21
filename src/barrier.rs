use std::sync::Arc;

use crate::sequence::Sequence;
use crate::sync::{AtomicBool, Ordering};
use crate::wait::{WaitResult, WaitStrategy};

pub(crate) struct SequenceBarrier<W: WaitStrategy> {
    deps: Vec<Arc<Sequence>>,
    wait: Arc<W>,
    alert: Arc<AtomicBool>,
}

impl<W: WaitStrategy> SequenceBarrier<W> {
    pub(crate) fn new(deps: Vec<Arc<Sequence>>, wait: Arc<W>, alert: Arc<AtomicBool>) -> Self {
        Self { deps, wait, alert }
    }

    #[inline]
    pub(crate) fn is_alerted(&self) -> bool {
        self.alert.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn register_current_thread(&self) {
        self.wait.register_current_thread();
    }

    #[inline]
    pub(crate) fn signal_all(&self) {
        self.wait.signal_all();
    }

    #[inline]
    pub(crate) fn wait_for(&self, target: i64) -> WaitResult {
        let mut attempt = 0u32;
        loop {
            if self.is_alerted() {
                return WaitResult::Alerted;
            }

            // Sequence::get performs the Acquire loads that make all writes
            // published by every dependency visible before a slot is read.
            let available = self
                .deps
                .iter()
                .map(|sequence| sequence.get())
                .min()
                .expect("sequence barrier must have at least one dependency");
            if available >= target {
                return WaitResult::Available(available);
            }

            self.wait.idle(attempt);
            attempt = attempt.saturating_add(1);
        }
    }
}
