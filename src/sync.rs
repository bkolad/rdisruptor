//! Single import point for every concurrency primitive the disruptor uses.
//!
//! Building with `--features loom` swaps each primitive for its loom
//! counterpart so the model checker can exhaustively explore interleavings
//! (see `tests/loom.rs`). Production builds re-export the std and
//! `atomic-wait` types unchanged.

#[cfg(not(feature = "loom"))]
mod imp {
    pub(crate) use atomic_wait::{wait, wake_all};
    pub(crate) use std::hint::spin_loop;
    pub(crate) use std::sync::atomic::{fence, AtomicBool, AtomicI64, AtomicU32, Ordering};
    pub(crate) use std::sync::{Arc, Mutex, MutexGuard};
    pub(crate) use std::thread;
    pub(crate) use std::thread::sleep;

    /// `UnsafeCell` behind loom's closure-based access API, so ring-slot
    /// reads and writes have one shape under both cfgs. Loom's `with_mut`
    /// flags any interleaving where two closures overlap on the same cell;
    /// this passthrough imposes no checks and no cost.
    pub(crate) struct SlotCell<T>(std::cell::UnsafeCell<T>);

    impl<T> SlotCell<T> {
        pub(crate) fn new(value: T) -> Self {
            Self(std::cell::UnsafeCell::new(value))
        }

        #[inline]
        pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }

        #[inline]
        pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}

#[cfg(feature = "loom")]
mod imp {
    pub(crate) use loom::cell::UnsafeCell as SlotCell;
    pub(crate) use loom::hint::spin_loop;
    pub(crate) use loom::sync::atomic::{fence, AtomicBool, AtomicI64, AtomicU32, Ordering};
    pub(crate) use loom::sync::{Arc, Mutex, MutexGuard};
    pub(crate) use loom::thread;

    /// Loom has no futex. Returning immediately is a spurious wakeup, which
    /// every `wait()` caller already tolerates by re-checking its condition;
    /// the yield gives the scheduler a point to run other threads.
    pub(crate) fn wait(_futex: &AtomicU32, _expected: u32) {
        loom::thread::yield_now();
    }

    pub(crate) fn wake_all(_futex: &AtomicU32) {}

    /// Loom has no clock; a yield models "some time passes".
    pub(crate) fn sleep(_duration: std::time::Duration) {
        loom::thread::yield_now();
    }
}

pub(crate) use imp::*;
