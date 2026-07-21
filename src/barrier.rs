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
    pub(crate) fn signal(&self) {
        self.wait.signal();
    }

    #[inline]
    fn poll(&self, target: i64) -> Option<WaitResult> {
        if self.is_alerted() {
            return Some(WaitResult::Alerted);
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
            Some(WaitResult::Available(available))
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn wait_for(&self, target: i64) -> WaitResult {
        loop {
            if let Some(result) = self.poll(target) {
                return result;
            }

            let mut observed = None;
            self.wait.wait_until(|| {
                if let Some(result) = self.poll(target) {
                    observed = Some(result);
                    true
                } else {
                    false
                }
            });

            // WaitStrategy is a safe extension point and may return without
            // invoking the predicate. Only a result produced by poll() can
            // authorize the processor to read a ring-buffer slot.
            if let Some(result) = observed {
                return result;
            }
        }
    }
}
