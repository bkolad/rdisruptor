use std::sync::Mutex;
use std::thread::{self, Thread};
use std::time::Duration;

pub(crate) enum WaitResult {
    Available(i64),
    Alerted,
}

/// Controls how a worker idles between unsuccessful availability checks.
///
/// Cursor reads, publication checks, and memory ordering remain internal to the
/// disruptor; implementations can affect latency and CPU usage, but not the
/// safety of ring-buffer access.
pub trait WaitStrategy: Send + Sync + 'static {
    /// Register the calling thread before it starts checking a condition that
    /// may require idling.
    ///
    /// Notification-based strategies use this hook to avoid missing a signal
    /// between the availability check and [`Self::idle`]. Polling strategies
    /// do not need to override it.
    #[inline]
    fn register_current_thread(&self) {}

    /// Idle after a failed availability check. `attempt` starts at zero for
    /// each wait and saturates at `u32::MAX`.
    fn idle(&self, attempt: u32);

    /// Wake threads that may be idling after sequence or alert state changed.
    ///
    /// Polling strategies do not need to override this method.
    #[inline]
    fn signal_all(&self) {}
}

/// Lowest latency, highest CPU usage. Pin the consumer thread.
pub struct BusySpin;

impl WaitStrategy for BusySpin {
    #[inline]
    fn idle(&self, _attempt: u32) {
        std::hint::spin_loop();
    }
}

/// Spin a fixed number of iterations, then yield. Lower CPU than busy-spin
/// at modest latency cost.
pub struct Yielding {
    spin_tries: u32,
}

impl Yielding {
    pub const DEFAULT_SPIN_TRIES: u32 = 100;

    pub fn new() -> Self {
        Self {
            spin_tries: Self::DEFAULT_SPIN_TRIES,
        }
    }

    pub fn with_spin_tries(spin_tries: u32) -> Self {
        Self { spin_tries }
    }
}

impl Default for Yielding {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitStrategy for Yielding {
    #[inline]
    fn idle(&self, attempt: u32) {
        if attempt < self.spin_tries {
            std::hint::spin_loop();
        } else {
            thread::yield_now();
        }
    }
}

/// Spin → yield → sleep. Lowest CPU at the highest latency cost.
pub struct Sleeping {
    spin_tries: u32,
    yield_tries: u32,
    sleep: Duration,
}

impl Sleeping {
    pub fn new() -> Self {
        Self {
            spin_tries: 100,
            yield_tries: 100,
            sleep: Duration::from_micros(50),
        }
    }

    pub fn with(sleep: Duration) -> Self {
        Self {
            spin_tries: 100,
            yield_tries: 100,
            sleep,
        }
    }
}

impl Default for Sleeping {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitStrategy for Sleeping {
    #[inline]
    fn idle(&self, attempt: u32) {
        if attempt < self.spin_tries {
            std::hint::spin_loop();
        } else if attempt < self.spin_tries.saturating_add(self.yield_tries) {
            thread::yield_now();
        } else {
            thread::sleep(self.sleep);
        }
    }
}

/// Block idle threads until publication, dependency progress, or shutdown.
///
/// This strategy uses [`Thread::park`] and [`Thread::unpark`] rather than
/// periodically polling. It therefore has very low idle CPU usage while still
/// waking promptly when disruptor state changes. Wakeups are global: all
/// registered consumer threads, plus any producer thread that encountered
/// backpressure, are unparked after each progress notification.
pub struct Blocking {
    threads: Mutex<Vec<Thread>>,
}

impl Blocking {
    pub fn new() -> Self {
        Self {
            threads: Mutex::new(Vec::new()),
        }
    }

    fn threads(&self) -> std::sync::MutexGuard<'_, Vec<Thread>> {
        self.threads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for Blocking {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitStrategy for Blocking {
    fn register_current_thread(&self) {
        let current = thread::current();
        let current_id = current.id();
        let mut threads = self.threads();
        if !threads
            .iter()
            .any(|registered| registered.id() == current_id)
        {
            threads.push(current);
        }
    }

    #[inline]
    fn idle(&self, _attempt: u32) {
        thread::park();
    }

    fn signal_all(&self) {
        for registered in self.threads().iter() {
            registered.unpark();
        }
    }
}
