use std::sync::{Mutex, MutexGuard};
use std::thread::{self, Thread};
use std::time::Duration;

use crate::sync::{fence, wait, wake_all, AtomicU32, Ordering};

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
    /// Wait until `check` reports that the caller's condition is satisfied.
    ///
    /// Implementations may call `check` as often as needed and may return
    /// spuriously. The disruptor validates availability itself after a
    /// spurious return, so a strategy can affect latency and CPU usage but
    /// cannot expose unpublished ring-buffer entries.
    fn wait_until<C>(&self, check: C)
    where
        C: FnMut() -> bool;

    /// Notify threads after sequence or alert state changed.
    ///
    /// This method may be called from any thread at any time. Polling
    /// strategies do not need to override it.
    #[inline]
    fn signal(&self) {}
}

/// Poll a disruptor-owned condition until it produces a result.
///
/// A wait strategy is allowed to return spuriously or without invoking its
/// predicate. Keeping this loop here ensures that only a value returned by
/// `poll` can authorize progress through the ring-buffer protocol.
#[inline]
pub(crate) fn wait_until_some<W, R, P>(wait: &W, mut poll: P) -> R
where
    W: WaitStrategy,
    P: FnMut() -> Option<R>,
{
    loop {
        if let Some(result) = poll() {
            return result;
        }

        let mut observed = None;
        wait.wait_until(|| {
            if observed.is_some() {
                return true;
            }

            if let Some(result) = poll() {
                observed = Some(result);
                true
            } else {
                false
            }
        });

        if let Some(result) = observed {
            return result;
        }
    }
}

/// Lowest latency, highest CPU usage. Pin the consumer thread.
pub struct BusySpin;

impl WaitStrategy for BusySpin {
    #[inline]
    fn wait_until<C>(&self, mut check: C)
    where
        C: FnMut() -> bool,
    {
        while !check() {
            std::hint::spin_loop();
        }
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
    fn wait_until<C>(&self, mut check: C)
    where
        C: FnMut() -> bool,
    {
        let mut attempt = 0u32;
        while !check() {
            if attempt < self.spin_tries {
                std::hint::spin_loop();
            } else {
                thread::yield_now();
            }
            attempt = attempt.saturating_add(1);
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
    fn wait_until<C>(&self, mut check: C)
    where
        C: FnMut() -> bool,
    {
        let mut attempt = 0u32;
        while !check() {
            if attempt < self.spin_tries {
                std::hint::spin_loop();
            } else if attempt < self.spin_tries.saturating_add(self.yield_tries) {
                thread::yield_now();
            } else {
                thread::sleep(self.sleep);
            }
            attempt = attempt.saturating_add(1);
        }
    }
}

struct ThreadRegistry {
    threads: Mutex<Vec<RegisteredThread>>,
}

struct RegisteredThread {
    thread: Thread,
    registrations: usize,
}

impl ThreadRegistry {
    fn new() -> Self {
        Self {
            threads: Mutex::new(Vec::new()),
        }
    }

    fn threads(&self) -> MutexGuard<'_, Vec<RegisteredThread>> {
        self.threads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn register_current_thread(&self) {
        let current = thread::current();
        let current_id = current.id();
        let mut threads = self.threads();
        if let Some(registered) = threads
            .iter_mut()
            .find(|registered| registered.thread.id() == current_id)
        {
            registered.registrations = registered
                .registrations
                .checked_add(1)
                .expect("thread registration count overflowed");
        } else {
            threads.push(RegisteredThread {
                thread: current,
                registrations: 1,
            });
        }
    }

    fn deregister_current_thread(&self) {
        let current_id = thread::current().id();
        let mut threads = self.threads();
        if let Some(pos) = threads
            .iter()
            .position(|registered| registered.thread.id() == current_id)
        {
            if threads[pos].registrations == 1 {
                threads.swap_remove(pos);
            } else {
                threads[pos].registrations -= 1;
            }
        }
    }

    fn signal_all(&self) {
        for registered in self.threads().iter() {
            registered.thread.unpark();
        }
    }
}

struct ThreadRegistration<'a> {
    registry: &'a ThreadRegistry,
}

impl<'a> ThreadRegistration<'a> {
    fn new(registry: &'a ThreadRegistry) -> Self {
        registry.register_current_thread();
        Self { registry }
    }
}

impl Drop for ThreadRegistration<'_> {
    fn drop(&mut self) {
        self.registry.deregister_current_thread();
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
    registry: ThreadRegistry,
}

impl Blocking {
    pub fn new() -> Self {
        Self {
            registry: ThreadRegistry::new(),
        }
    }
}

impl Default for Blocking {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitStrategy for Blocking {
    fn wait_until<C>(&self, mut check: C)
    where
        C: FnMut() -> bool,
    {
        // signal() always takes this same registry mutex. If a notification
        // finishes before registration, acquiring the mutex here makes its
        // preceding state change visible to the first check. Otherwise the
        // registered handle receives an unpark token.
        let _registration = ThreadRegistration::new(&self.registry);
        while !check() {
            thread::park();
        }
    }

    fn signal(&self) {
        self.registry.signal_all();
    }
}

/// Spin, optionally yield, then block on an atomic generation until
/// publication, dependency progress, or shutdown.
///
/// The generation word provides the compare-before-sleep operation through
/// Linux futexes, macOS libc++ atomic wait, and equivalent primitives on other
/// supported platforms. Its low bit is an edge-triggered signal gate: one
/// signal wakes all armed threads, and later signals avoid wake syscalls until
/// a thread explicitly rearms before checking its condition again.
///
/// The atomic-wait backend requires macOS 11 or newer. If the condition becomes
/// ready after a thread arms the gate but before it sleeps, the thread returns
/// without clearing the shared bit because other waiters may still need it.
/// The next signal may therefore perform one harmless wake even when no thread
/// is asleep; this is expected and can appear as a stray syscall in profiles.
pub struct Parking {
    state: AtomicU32,
    spin_tries: u32,
    yield_tries: u32,
}

const PARKING_ARMED: u32 = 1;
const PARKING_GENERATION_STEP: u32 = 2;

impl Parking {
    pub const DEFAULT_SPIN_TRIES: u32 = 100;
    pub const DEFAULT_YIELD_TRIES: u32 = 0;

    pub fn new() -> Self {
        Self::with_tries(Self::DEFAULT_SPIN_TRIES, Self::DEFAULT_YIELD_TRIES)
    }

    /// Configure how many failed checks spin and yield before the thread
    /// enters the gated parking phase. Passing zero for both parks
    /// immediately.
    pub fn with_tries(spin_tries: u32, yield_tries: u32) -> Self {
        Self {
            state: AtomicU32::new(0),
            spin_tries,
            yield_tries,
        }
    }
}

impl Default for Parking {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitStrategy for Parking {
    fn wait_until<C>(&self, mut check: C)
    where
        C: FnMut() -> bool,
    {
        if check() {
            return;
        }

        for _ in 0..self.spin_tries {
            std::hint::spin_loop();
            if check() {
                return;
            }
        }

        for _ in 0..self.yield_tries {
            thread::yield_now();
            if check() {
                return;
            }
        }

        loop {
            // Arm before the final check. The fence pairs with signal()'s
            // fence: either signal observes the armed bit and wakes us, or
            // this final check observes the preceding disruptor state change.
            let expected = self.state.fetch_or(PARKING_ARMED, Ordering::SeqCst) | PARKING_ARMED;
            fence(Ordering::SeqCst);
            if check() {
                return;
            }

            wait(&self.state, expected);
        }
    }

    fn signal(&self) {
        // The SC fences close the announce/check versus mutate/signal
        // store-load race. With no armed waiter, the fast path is one fence and
        // one load; it does not modify a cache line shared with the waiter.
        fence(Ordering::SeqCst);
        let mut current = self.state.load(Ordering::Relaxed);

        while current & PARKING_ARMED != 0 {
            // Clear the one-shot gate and advance the generation atomically.
            // Addition by two preserves the armed bit across wraparound until
            // the mask clears it. Only the successful signaler performs the
            // OS wake; concurrent signalers observe the cleared gate.
            let next = current.wrapping_add(PARKING_GENERATION_STEP) & !PARKING_ARMED;
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    wake_all(&self.state);
                    return;
                }
                Err(observed) => current = observed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    use super::{
        wait_until_some, Blocking, Ordering, Parking, ThreadRegistry, WaitStrategy, PARKING_ARMED,
    };

    struct SpuriousReturn;

    impl WaitStrategy for SpuriousReturn {
        fn wait_until<C>(&self, _check: C)
        where
            C: FnMut() -> bool,
        {
        }
    }

    #[test]
    fn wait_until_some_repolls_after_spurious_strategy_returns() {
        let mut polls = 0;
        let result = wait_until_some(&SpuriousReturn, || {
            polls += 1;
            (polls == 3).then_some(42)
        });

        assert_eq!(result, 42);
        assert_eq!(polls, 3);
    }

    #[test]
    fn registry_keeps_nested_registrations_until_each_is_released() {
        let registry = ThreadRegistry::new();

        registry.register_current_thread();
        registry.register_current_thread();
        registry.deregister_current_thread();

        let threads = registry.threads();
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].registrations, 1);
        drop(threads);

        registry.deregister_current_thread();
        assert!(registry.threads().is_empty());
    }

    #[test]
    fn blocking_deregisters_when_check_panics() {
        let wait = Blocking::new();

        let result = catch_unwind(AssertUnwindSafe(|| {
            wait.wait_until(|| panic!("check failed"));
        }));

        assert!(result.is_err());
        assert!(wait.registry.threads().is_empty());
    }

    #[test]
    fn parking_check_panic_does_not_leave_the_signal_gate_stuck() {
        let wait = Parking::with_tries(0, 0);
        let checks = Cell::new(0);

        let result = catch_unwind(AssertUnwindSafe(|| {
            wait.wait_until(|| {
                checks.set(checks.get() + 1);
                if checks.get() == 1 {
                    false
                } else {
                    panic!("check failed");
                }
            });
        }));

        assert!(result.is_err());
        assert_ne!(wait.state.load(Ordering::Relaxed) & PARKING_ARMED, 0);

        wait.signal();
        assert_eq!(wait.state.load(Ordering::Relaxed) & PARKING_ARMED, 0);
    }

    #[test]
    fn parking_does_not_lose_signal_between_final_check_and_wait() {
        let wait = Arc::new(Parking::with_tries(0, 0));
        let ready = Arc::new(AtomicBool::new(false));
        let checks = Arc::new(AtomicUsize::new(0));
        let (armed_tx, armed_rx) = mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = mpsc::sync_channel(1);
        let (done_tx, done_rx) = mpsc::sync_channel(1);

        let waiter_wait = Arc::clone(&wait);
        let waiter_ready = Arc::clone(&ready);
        let waiter_checks = Arc::clone(&checks);
        let waiter = std::thread::spawn(move || {
            waiter_wait.wait_until(|| {
                let check = waiter_checks.fetch_add(1, Ordering::Relaxed);
                if check == 0 {
                    return false;
                }
                if check == 1 {
                    armed_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                    // Force the signal into the check-to-wait window. The
                    // changed generation must make atomic_wait return without
                    // relying on a wake token.
                    return false;
                }
                waiter_ready.load(Ordering::Acquire)
            });
            done_tx.send(()).unwrap();
        });

        armed_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("waiter did not arm");
        ready.store(true, Ordering::Release);
        wait.signal();
        resume_tx.send(()).unwrap();

        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("waiter lost a signal before atomic_wait");
        waiter.join().unwrap();
    }
}
