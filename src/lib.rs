//! LMAX Disruptor PoC for Rust.
//!
//! Public surface:
//!
//! - [`Consumer`] and [`WaitStrategy`] — the trait extension points.
//! - [`DisruptorBuilder`] — declarative DAG: `consumer(...)` and
//!   `consumer_after(deps, ...)`.
//! - [`Disruptor`] — owns the consumer threads; can print its topology; hands
//!   out a [`SingleProducer`].
//! - [`spsc`] — thin convenience wrapper for the single-consumer case.

mod barrier;
mod builder;
mod consumer;
mod processor;
mod producer;
mod ring;
mod sequence;
mod sync;
mod wait;

pub use builder::{BuildError, Disruptor, DisruptorBuilder, ShutdownError};
pub use consumer::{Consumer, MutConsumer};
pub use producer::{PublishError, SingleProducer};
pub use wait::{Blocking, BusySpin, Parking, Sleeping, WaitStrategy, Yielding};

/// Convenience for the single-producer / single-consumer case. Equivalent to
/// `DisruptorBuilder::new().capacity(cap).consumer(consumer).build(wait)`.
///
/// # Errors
///
/// Returns [`BuildError::InvalidCapacity`] when `capacity` is less than two,
/// is not a power of two, or cannot fit in an `i64` sequence range.
pub fn spsc<T, C, W>(capacity: usize, wait: W, consumer: C) -> Result<Disruptor<T, W>, BuildError>
where
    T: Default + Send + Sync + 'static,
    C: Consumer<T> + 'static,
    W: WaitStrategy,
{
    DisruptorBuilder::<T>::new()
        .capacity(capacity)
        .consumer(consumer)
        .build(wait)
}
