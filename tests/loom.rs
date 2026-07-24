//! Loom model checks for the ring-buffer protocol.
//!
//! Build with the `loom` feature:
//!
//! ```text
//! cargo test --release --features loom --test loom
//! ```
//!
//! Each model is deliberately tiny — capacity 2, a handful of events, at most
//! three consumer threads — because loom explores interleavings exhaustively
//! (up to the preemption bound). What these models prove that the std tests
//! cannot: loom's `UnsafeCell` fails any schedule where a producer's slot
//! write overlaps a consumer's slot read, and loom's atomics fail any
//! schedule where a load observes a value the memory model would not allow.
#![cfg(feature = "loom")]

use loom::sync::atomic::{AtomicI64, Ordering};
use loom::sync::{Arc, Mutex};

use rdisruptor::{
    spsc, Blocking, BusySpin, Consumer, DisruptorBuilder, Parking, PublishError, WaitStrategy,
};

/// Exhaustive exploration of the whole disruptor is intractable; a small
/// preemption bound is loom's recommended trade-off (the overwhelming
/// majority of concurrency bugs need at most two forced preemptions).
/// `LOOM_MAX_PREEMPTIONS` still overrides this when set.
fn model<F>(f: F)
where
    F: Fn() + Sync + Send + 'static,
{
    let mut builder = loom::model::Builder::new();
    if builder.preemption_bound.is_none() {
        builder.preemption_bound = Some(2);
    }
    builder.check(f);
}

struct Collect {
    out: Arc<Mutex<Vec<u64>>>,
}

impl Consumer<u64> for Collect {
    fn name(&self) -> &str {
        "collect"
    }

    fn on_event(&mut self, ev: &u64, _seq: i64, _eob: bool) {
        self.out.lock().unwrap().push(*ev);
    }
}

/// Publish `n` events through a capacity-2 ring and require every one to
/// arrive in order. With n > 2 the producer must block on the gating
/// sequence, so this covers claim/publish, wraparound, and backpressure.
fn assert_spsc_delivers_in_order<W: WaitStrategy>(make_wait: fn() -> W, n: u64) {
    model(move || {
        let out = Arc::new(Mutex::new(Vec::new()));
        let consumer = Collect {
            out: Arc::clone(&out),
        };

        let mut disruptor = spsc::<u64, _, _>(2, make_wait(), consumer).unwrap();
        let mut producer = disruptor.producer();
        for i in 0..n {
            producer.publish(|slot| *slot = i).unwrap();
        }

        // Shutdown does not drain, so wait until the consumer has seen
        // everything. Yielding lets the loom scheduler run the consumer.
        while out.lock().unwrap().len() < n as usize {
            loom::thread::yield_now();
        }
        disruptor.shutdown_or_panic();

        let seen = out.lock().unwrap();
        assert_eq!(*seen, (0..n).collect::<Vec<_>>());
    });
}

#[test]
fn spsc_busy_spin_delivers_in_order_through_wraparound() {
    assert_spsc_delivers_in_order(|| BusySpin, 4);
}

#[test]
fn spsc_parking_delivers_in_order_through_wraparound() {
    // Parking's arm/signal generation protocol adds atomics to the state
    // space, so this model uses fewer events than the busy-spin one.
    assert_spsc_delivers_in_order(|| Parking::with_tries(0, 0), 3);
}

#[test]
fn spsc_blocking_delivers_in_order_through_wraparound() {
    // Exercises park/unpark and the thread-registry mutex.
    assert_spsc_delivers_in_order(Blocking::new, 3);
}

/// A consumer that records its own progress in a cursor and asserts that all
/// upstream cursors are at least as far along as the event it receives.
struct Chained {
    name: &'static str,
    upstream: Vec<Arc<AtomicI64>>,
    cursor: Arc<AtomicI64>,
}

impl Consumer<u64> for Chained {
    fn name(&self) -> &str {
        self.name
    }

    fn on_event(&mut self, _ev: &u64, seq: i64, _eob: bool) {
        for up in &self.upstream {
            assert!(
                up.load(Ordering::Acquire) >= seq,
                "{}: upstream behind at seq {seq}",
                self.name
            );
        }
        self.cursor.store(seq, Ordering::Release);
    }
}

/// Two chained consumers: the barrier must never release an event to `b`
/// before `a` has processed it, in any interleaving.
///
/// Uses `Blocking` rather than `BusySpin`: with three threads, a spinning
/// downstream consumer exceeds loom's branch budget, whereas `park()` is a
/// real blocking point for loom's scheduler.
#[test]
fn dependency_chain_never_overtakes_upstream() {
    model(|| {
        let a_cursor = Arc::new(AtomicI64::new(-1));
        let b_cursor = Arc::new(AtomicI64::new(-1));

        let a = Chained {
            name: "a",
            upstream: vec![],
            cursor: Arc::clone(&a_cursor),
        };
        let b = Chained {
            name: "b",
            upstream: vec![Arc::clone(&a_cursor)],
            cursor: Arc::clone(&b_cursor),
        };

        let mut disruptor = DisruptorBuilder::<u64>::new()
            .capacity(2)
            .consumer(a)
            .consumer_after(["a"], b)
            .build(Blocking::new())
            .unwrap();

        let mut producer = disruptor.producer();
        for i in 0..2u64 {
            producer.publish(|slot| *slot = i).unwrap();
        }

        while b_cursor.load(Ordering::Acquire) < 1 {
            loom::thread::yield_now();
        }
        disruptor.shutdown_or_panic();
    });
}

/// Shutdown may race a consumer mid-batch; afterwards the producer must be
/// refused and the consumer must have seen an in-order prefix of the events.
#[test]
fn shutdown_races_cleanly_and_refuses_later_publishes() {
    model(|| {
        let out = Arc::new(Mutex::new(Vec::new()));
        let consumer = Collect {
            out: Arc::clone(&out),
        };

        let mut disruptor = spsc::<u64, _, _>(2, BusySpin, consumer).unwrap();
        let mut producer = disruptor.producer();
        producer.publish(|slot| *slot = 0).unwrap();
        producer.publish(|slot| *slot = 1).unwrap();

        // No drain barrier: the consumer may have seen 0, 1, or 2 events.
        disruptor.shutdown_or_panic();

        let seen = out.lock().unwrap();
        assert!(seen.len() <= 2);
        for (i, v) in seen.iter().enumerate() {
            assert_eq!(*v as usize, i);
        }
        drop(seen);

        assert_eq!(
            producer.publish(|slot| *slot = 9),
            Err(PublishError::Shutdown)
        );
    });
}
