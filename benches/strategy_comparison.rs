//! Compares CPU usage and saturated throughput for every built-in wait
//! strategy.
//!
//! Run with:
//!
//! ```text
//! cargo bench --bench strategy_comparison
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rdisruptor::{
    Blocking, BusySpin, Consumer, Disruptor, DisruptorBuilder, Parking, Sleeping, WaitStrategy,
    Yielding,
};

const CAPACITY: usize = 1_024;
const SETTLE_TIME: Duration = Duration::from_millis(20);
const IDLE_TIME: Duration = Duration::from_secs(2);
const SPARSE_EVENTS: u64 = 2_000;
const SPARSE_INTERVAL: Duration = Duration::from_millis(1);
const SATURATED_WARMUP_EVENTS: u64 = 100_000;
const SATURATED_BATCH_EVENTS: u64 = 1_000_000;
const SATURATED_MIN_TIME: Duration = Duration::from_secs(2);
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(30);

struct Counter {
    processed: Arc<AtomicU64>,
}

impl Consumer<u64> for Counter {
    fn name(&self) -> &str {
        "counter"
    }

    #[inline]
    fn on_event(&mut self, _event: &u64, _sequence: i64, _end_of_batch: bool) {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
struct Measurement {
    elapsed: Duration,
    cpu: Option<Duration>,
}

impl Measurement {
    fn cpu_percent(self) -> Option<f64> {
        self.cpu
            .map(|cpu| 100.0 * cpu.as_secs_f64() / self.elapsed.as_secs_f64())
    }
}

struct StrategyResult {
    name: &'static str,
    idle: Measurement,
    sparse: Measurement,
    saturated: SaturatedMeasurement,
}

impl StrategyResult {
    fn throughput(&self) -> f64 {
        self.saturated.events as f64 / self.saturated.measurement.elapsed.as_secs_f64()
    }
}

struct SaturatedMeasurement {
    measurement: Measurement,
    events: u64,
}

fn main() {
    eprintln!(
        "Running each strategy for 2 s idle, 2,000 sparse events, and at least 2 s saturated..."
    );

    let results = [
        benchmark_strategy("BusySpin", || BusySpin),
        benchmark_strategy("Yielding", Yielding::new),
        benchmark_strategy("Sleeping", Sleeping::new),
        benchmark_strategy("Blocking", Blocking::new),
        benchmark_strategy("Parking::with_tries(0, 0)", || Parking::with_tries(0, 0)),
        benchmark_strategy("Parking::with_tries(100, 0)", || {
            Parking::with_tries(100, 0)
        }),
        benchmark_strategy("Parking::with_tries(100, 100)", || {
            Parking::with_tries(100, 100)
        }),
    ];

    let busy_spin_throughput = results[0].throughput();

    println!("| Strategy | Idle CPU | Sparse CPU | Saturated CPU | Throughput | vs BusySpin |");
    println!("|---|---:|---:|---:|---:|---:|");
    for result in results {
        println!(
            "| {} | {} | {} | {} | {} | {:.1}% |",
            result.name,
            format_cpu(result.idle.cpu_percent()),
            format_cpu(result.sparse.cpu_percent()),
            format_cpu(result.saturated.measurement.cpu_percent()),
            format_rate(result.throughput()),
            100.0 * result.throughput() / busy_spin_throughput,
        );
    }

    println!();
    println!("CPU is total process CPU divided by wall time; saturated results can approach 200% ");
    println!("because the producer and consumer run on separate threads.");
}

fn benchmark_strategy<W, F>(name: &'static str, make_wait: F) -> StrategyResult
where
    W: WaitStrategy,
    F: Fn() -> W,
{
    eprintln!("  {name}");
    StrategyResult {
        name,
        idle: benchmark_idle(make_wait()),
        sparse: benchmark_sparse(make_wait()),
        saturated: benchmark_saturated(make_wait()),
    }
}

fn setup<W: WaitStrategy>(wait: W) -> (Disruptor<u64, W>, Arc<AtomicU64>) {
    let processed = Arc::new(AtomicU64::new(0));
    let disruptor = DisruptorBuilder::<u64>::new()
        .capacity(CAPACITY)
        .consumer(Counter {
            processed: Arc::clone(&processed),
        })
        .build(wait)
        .expect("the benchmark disruptor should build");
    (disruptor, processed)
}

fn benchmark_idle<W: WaitStrategy>(wait: W) -> Measurement {
    let (disruptor, _processed) = setup(wait);
    thread::sleep(SETTLE_TIME);

    let measurement = measure(|| thread::sleep(IDLE_TIME));
    disruptor.shutdown_or_panic();
    measurement
}

fn benchmark_sparse<W: WaitStrategy>(wait: W) -> Measurement {
    let (mut disruptor, processed) = setup(wait);
    let mut producer = disruptor.producer();
    thread::sleep(SETTLE_TIME);

    let measurement = measure(|| {
        for value in 0..SPARSE_EVENTS {
            producer
                .publish(|slot| *slot = value)
                .expect("the benchmark consumer should be running");
            thread::sleep(SPARSE_INTERVAL);
        }
        wait_for_count(&processed, SPARSE_EVENTS);
    });

    drop(producer);
    disruptor.shutdown_or_panic();
    measurement
}

fn benchmark_saturated<W: WaitStrategy>(wait: W) -> SaturatedMeasurement {
    let (mut disruptor, processed) = setup(wait);
    let mut producer = disruptor.producer();
    thread::sleep(SETTLE_TIME);

    for value in 0..SATURATED_WARMUP_EVENTS {
        producer
            .publish(|slot| *slot = value)
            .expect("the benchmark consumer should be running");
    }
    wait_for_count(&processed, SATURATED_WARMUP_EVENTS);

    let mut events = 0;
    let measurement = measure(|| {
        let saturation_start = Instant::now();
        while events == 0 || saturation_start.elapsed() < SATURATED_MIN_TIME {
            for value in 0..SATURATED_BATCH_EVENTS {
                producer
                    .publish(|slot| *slot = value)
                    .expect("the benchmark consumer should be running");
            }
            events += SATURATED_BATCH_EVENTS;
        }

        let target = SATURATED_WARMUP_EVENTS + events;
        wait_for_count(&processed, target);
    });

    drop(producer);
    disruptor.shutdown_or_panic();
    SaturatedMeasurement {
        measurement,
        events,
    }
}

fn wait_for_count(processed: &AtomicU64, expected: u64) {
    let deadline = Instant::now() + COMPLETION_TIMEOUT;
    while processed.load(Ordering::Acquire) < expected {
        assert!(
            Instant::now() < deadline,
            "consumer did not process {expected} events within the timeout"
        );
        thread::yield_now();
    }
}

fn measure(work: impl FnOnce()) -> Measurement {
    let cpu_start = process_cpu_time();
    let wall_start = Instant::now();
    work();
    let elapsed = wall_start.elapsed();
    let cpu = cpu_start
        .zip(process_cpu_time())
        .map(|(start, end)| end.saturating_sub(start));
    Measurement { elapsed, cpu }
}

fn format_cpu(cpu_percent: Option<f64>) -> String {
    match cpu_percent {
        Some(value) => format!("{value:.1}%"),
        None => "n/a".to_string(),
    }
}

fn format_rate(events_per_second: f64) -> String {
    if events_per_second >= 1_000_000.0 {
        format!("{:.1}M events/s", events_per_second / 1_000_000.0)
    } else if events_per_second >= 1_000.0 {
        format!("{:.1}K events/s", events_per_second / 1_000.0)
    } else {
        format!("{events_per_second:.1} events/s")
    }
}

#[cfg(unix)]
fn process_cpu_time() -> Option<Duration> {
    use std::mem::MaybeUninit;

    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the supplied rusage on success, and the
    // return value is checked before assume_init.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(
        status,
        0,
        "getrusage failed: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: a zero status means getrusage initialized the whole structure.
    let usage = unsafe { usage.assume_init() };
    Some(timeval_duration(usage.ru_utime) + timeval_duration(usage.ru_stime))
}

#[cfg(unix)]
fn timeval_duration(value: libc::timeval) -> Duration {
    let seconds = u64::try_from(value.tv_sec).expect("CPU seconds should not be negative");
    let micros = u64::try_from(value.tv_usec).expect("CPU microseconds should not be negative");
    Duration::from_secs(seconds) + Duration::from_micros(micros)
}

#[cfg(not(unix))]
fn process_cpu_time() -> Option<Duration> {
    None
}
