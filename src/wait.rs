use std::thread;
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
    /// Idle after a failed availability check. `attempt` starts at zero for
    /// each wait and saturates at `u32::MAX`.
    fn idle(&self, attempt: u32);
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
