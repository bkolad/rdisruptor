use std::sync::Arc;

use crate::sequence::Sequence;
use crate::sync::{AtomicBool, Ordering};
use crate::wait::{wait_until_some, WaitResult, WaitStrategy};

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
        wait_until_some(self.wait.as_ref(), || self.poll(target))
    }
}
