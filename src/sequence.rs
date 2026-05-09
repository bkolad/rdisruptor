use crate::sync::{AtomicI64, Ordering};

pub(crate) const INITIAL: i64 = -1;

#[repr(align(64))]
pub(crate) struct Sequence {
    value: AtomicI64,
}

impl Sequence {
    pub(crate) fn new(initial: i64) -> Self {
        Self {
            value: AtomicI64::new(initial),
        }
    }

    #[inline]
    pub(crate) fn get(&self) -> i64 {
        self.value.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn set(&self, v: i64) {
        self.value.store(v, Ordering::Release);
    }
}

impl Default for Sequence {
    fn default() -> Self {
        Self::new(INITIAL)
    }
}
