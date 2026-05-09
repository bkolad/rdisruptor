//! Throughput benchmarks: rdisruptor.
//! Run with: `cargo bench`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use rdisruptor::{BusySpin, Consumer, DisruptorBuilder};

const N: u64 = 1_000_000;

// --- SPSC --------------------------------------------------------------------

fn spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_1M_u64");
    group.throughput(Throughput::Elements(N));
    group.sample_size(10);

    group.bench_function(BenchmarkId::new("rdisruptor", "BusySpin"), |b| {
        b.iter(|| {
            #[derive(Clone)]
            struct Sink {
                name: &'static str,
                count: Arc<AtomicU64>,
            }
            impl Sink {
                fn new(name: &'static str, count: Arc<AtomicU64>) -> Self {
                    Self { name, count }
                }
            }
            impl Consumer<u64> for Sink {
                #[inline]
                fn name(&self) -> &str {
                    self.name
                }

                #[inline]
                fn on_event(&mut self, _ev: &u64, _seq: i64, _eob: bool) {
                    self.count.fetch_add(1, Ordering::Relaxed);
                }
            }
            let count = Arc::new(AtomicU64::new(0));
            let mut disruptor = DisruptorBuilder::<u64>::new()
                .capacity(1024)
                .consumer(Sink::new("sink", Arc::clone(&count)))
                .build(BusySpin)
                .unwrap();
            let mut p = disruptor.producer();
            for i in 0..N {
                p.publish(|slot| *slot = i).unwrap();
            }
            while count.load(Ordering::Acquire) < N {
                std::hint::spin_loop();
            }
            disruptor.shutdown_or_panic();
        });
    });

    group.finish();
}

// --- 4-stage DAG -------------------------------------------------------------

fn dag(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_1M_u64");
    group.throughput(Throughput::Elements(N));
    group.sample_size(10);

    group.bench_function(BenchmarkId::new("rdisruptor", "fanout-fanin"), |b| {
        b.iter(|| {
            #[derive(Clone)]
            struct Tally {
                name: &'static str,
                c: Arc<AtomicU64>,
            }
            impl Tally {
                fn new(name: &'static str, c: Arc<AtomicU64>) -> Self {
                    Self { name, c }
                }
            }
            impl Consumer<u64> for Tally {
                #[inline]
                fn name(&self) -> &str {
                    self.name
                }

                #[inline]
                fn on_event(&mut self, _ev: &u64, _seq: i64, _eob: bool) {
                    self.c.fetch_add(1, Ordering::Relaxed);
                }
            }
            let leaf = Arc::new(AtomicU64::new(0));
            let a = Tally::new("a", Arc::new(AtomicU64::new(0)));
            let b = Tally::new("b", Arc::new(AtomicU64::new(0)));
            let c = Tally::new("c", Arc::new(AtomicU64::new(0)));
            let d = Tally::new("d", Arc::clone(&leaf));

            let mut disruptor = DisruptorBuilder::<u64>::new()
                .capacity(1024)
                .consumer(a.clone())
                .consumer(b.clone())
                .consumer_after([a.name(), b.name()], c.clone())
                .consumer_after([c.name()], d)
                .build(BusySpin)
                .unwrap();
            let mut p = disruptor.producer();
            for i in 0..N {
                p.publish(|slot| *slot = i).unwrap();
            }
            while leaf.load(Ordering::Acquire) < N {
                std::hint::spin_loop();
            }
            disruptor.shutdown_or_panic();
        });
    });

    group.finish();
}

criterion_group!(benches, spsc, dag);
criterion_main!(benches);
