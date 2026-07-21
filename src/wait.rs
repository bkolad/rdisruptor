use std::sync::{Mutex, MutexGuard};
use std::thread::{self, Thread};
use std::time::Duration;

use crate::sync::{fence, AtomicUsize, Ordering};

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

/// Spin, then yield, then park until publication, dependency progress, or
/// shutdown.
///
/// The spin and yield phases avoid scheduler round trips for short gaps. Once
/// the thread reaches the cold phase it announces itself before its final
/// condition check. [`Self::signal`] uses that announcement as a gate, so the
/// busy path avoids the registry mutex and thread wakeups when nobody is
/// parked. Even on that fast path, `signal` executes a sequentially consistent
/// fence and one relaxed waiter-count load to prevent lost wakeups.
pub struct Parking {
    waiters: AtomicUsize,
    registry: ThreadRegistry,
    spin_tries: u32,
    yield_tries: u32,
}

impl Parking {
    pub const DEFAULT_SPIN_TRIES: u32 = 100;
    pub const DEFAULT_YIELD_TRIES: u32 = 100;

    pub fn new() -> Self {
        Self::with_tries(Self::DEFAULT_SPIN_TRIES, Self::DEFAULT_YIELD_TRIES)
    }

    /// Configure how many failed checks spin and yield before the thread
    /// enters the gated parking phase. Passing zero for both parks
    /// immediately.
    pub fn with_tries(spin_tries: u32, yield_tries: u32) -> Self {
        Self {
            waiters: AtomicUsize::new(0),
            registry: ThreadRegistry::new(),
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

struct WaiterAnnouncement<'a> {
    waiters: &'a AtomicUsize,
}

impl<'a> WaiterAnnouncement<'a> {
    fn new(waiters: &'a AtomicUsize) -> Self {
        waiters.fetch_add(1, Ordering::SeqCst);
        Self { waiters }
    }
}

impl Drop for WaiterAnnouncement<'_> {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

struct ColdRegistration<'a> {
    // Fields drop in declaration order: leave the registry before lowering
    // the waiter gate.
    _thread: ThreadRegistration<'a>,
    _announcement: WaiterAnnouncement<'a>,
}

impl<'a> ColdRegistration<'a> {
    fn new(parking: &'a Parking) -> Self {
        // Announce before joining the registry, then fence after insertion and
        // before the final predicate check. If registry insertion panics, the
        // announcement guard still restores the waiter count.
        let announcement = WaiterAnnouncement::new(&parking.waiters);
        let thread = ThreadRegistration::new(&parking.registry);
        fence(Ordering::SeqCst);
        Self {
            _thread: thread,
            _announcement: announcement,
        }
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

        let _registration = ColdRegistration::new(self);
        while !check() {
            thread::park();
        }
    }

    fn signal(&self) {
        // Every state mutation that can satisfy a waiter is sequenced before
        // this fence. Together with the waiter's fence after registration,
        // the SC order guarantees one of two outcomes: this load observes the
        // waiter and signals it, or the waiter's final check observes the state
        // mutation and never parks. If signaling reaches the registry before
        // insertion, their shared mutex orders the mutation before that final
        // check; replacing it with unrelated locks would break the protocol.
        fence(Ordering::SeqCst);
        if self.waiters.load(Ordering::Relaxed) != 0 {
            self.registry.signal_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    use super::{Blocking, Ordering, Parking, ThreadRegistry, WaitStrategy};

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
    fn parking_cold_registration_is_released_when_check_panics() {
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
        assert_eq!(wait.waiters.load(Ordering::SeqCst), 0);
        assert!(wait.registry.threads().is_empty());
    }

    #[test]
    fn parking_signal_does_not_lock_registry_without_waiters() {
        let wait = Arc::new(Parking::new());
        let registry_guard = wait.registry.threads();
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let signaler_wait = Arc::clone(&wait);
        let signaler = std::thread::spawn(move || {
            signaler_wait.signal();
            done_tx.send(()).unwrap();
        });

        let result = done_rx.recv_timeout(Duration::from_secs(5));
        drop(registry_guard);
        signaler.join().unwrap();

        result.expect("signal locked the empty waiter registry");
    }
}
