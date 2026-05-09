//! Builder validation tests — pure validation, no threads spawned.

use rdisruptor::{BuildError, BusySpin, Consumer, DisruptorBuilder};

#[derive(Clone, Copy)]
struct Noop {
    name: &'static str,
}

impl Noop {
    fn new(name: &'static str) -> Self {
        Self { name }
    }
}

impl Consumer<u64> for Noop {
    fn name(&self) -> &str {
        self.name
    }

    fn on_event(&mut self, _ev: &u64, _seq: i64, _eob: bool) {}
}

fn assert_err<T>(res: Result<T, BuildError>, want: BuildError) {
    match res {
        Ok(_) => panic!("expected Err({want:?}), got Ok"),
        Err(e) => assert_eq!(e, want),
    }
}

#[test]
fn missing_capacity_errors() {
    let a = Noop::new("a");
    let res = DisruptorBuilder::<u64>::new().consumer(a).build(BusySpin);
    assert_err(res, BuildError::MissingCapacity);
}

#[test]
fn invalid_capacity_errors() {
    for capacity in [0, 1, 3] {
        let res = DisruptorBuilder::<u64>::new()
            .capacity(capacity)
            .consumer(Noop::new("a"))
            .build(BusySpin);
        assert_err(res, BuildError::InvalidCapacity { capacity });
    }
}

#[cfg(target_pointer_width = "64")]
#[test]
fn capacity_that_does_not_fit_in_i64_errors() {
    let capacity = 1usize << 63;
    let res = DisruptorBuilder::<u64>::new()
        .capacity(capacity)
        .consumer(Noop::new("a"))
        .build(BusySpin);
    assert_err(res, BuildError::InvalidCapacity { capacity });
}

#[test]
fn minimum_capacity_builds() {
    let disruptor = DisruptorBuilder::<u64>::new()
        .capacity(2)
        .consumer(Noop::new("a"))
        .build(BusySpin)
        .unwrap();
    disruptor.shutdown_or_panic();
}

#[test]
fn zero_max_batch_size_errors() {
    let a = Noop::new("a");
    let res = DisruptorBuilder::<u64>::new()
        .capacity(8)
        .max_batch_size(0)
        .consumer(a)
        .build(BusySpin);
    assert_err(res, BuildError::InvalidMaxBatchSize);
}

#[test]
fn empty_dag_errors() {
    let res = DisruptorBuilder::<u64>::new().capacity(8).build(BusySpin);
    assert_err(res, BuildError::EmptyDag);
}

#[test]
fn duplicate_name_errors() {
    let first = Noop::new("dup");
    let second = Noop::new("dup");
    let res = DisruptorBuilder::<u64>::new()
        .capacity(8)
        .consumer(first)
        .consumer(second)
        .build(BusySpin);
    assert_err(res, BuildError::DuplicateName(first.name().into()));
}

#[test]
fn consumer_name_with_nul_errors() {
    let res = DisruptorBuilder::<u64>::new()
        .capacity(8)
        .consumer(Noop::new("valid"))
        .consumer(Noop::new("bad\0name"))
        .build(BusySpin);
    assert_err(
        res,
        BuildError::InvalidConsumerName {
            consumer: "bad\0name".into(),
        },
    );
}

#[test]
fn unknown_dependency_errors() {
    let a = Noop::new("a");
    let b = Noop::new("b");
    let missing = Noop::new("nonexistent");
    let res = DisruptorBuilder::<u64>::new()
        .capacity(8)
        .consumer(a)
        .consumer_after([missing.name()], b)
        .build(BusySpin);
    assert_err(
        res,
        BuildError::UnknownDependency {
            consumer: b.name().into(),
            dep: missing.name().into(),
        },
    );
}

#[test]
fn cycle_errors() {
    // a -> b -> c -> a
    let a = Noop::new("a");
    let b = Noop::new("b");
    let c = Noop::new("c");
    let res = DisruptorBuilder::<u64>::new()
        .capacity(8)
        .consumer_after([c.name()], a)
        .consumer_after([a.name()], b)
        .consumer_after([b.name()], c)
        .build(BusySpin);
    assert_err(res, BuildError::Cycle);
}

#[test]
fn self_loop_is_a_cycle() {
    let a = Noop::new("a");
    let res = DisruptorBuilder::<u64>::new()
        .capacity(8)
        .consumer_after([a.name()], a)
        .build(BusySpin);
    assert_err(res, BuildError::Cycle);
}

#[test]
fn diamond_dag_builds_and_runs() {
    let a = Noop::new("a");
    let b = Noop::new("b");
    let c = Noop::new("c");
    let d = Noop::new("d");
    let mut disruptor = DisruptorBuilder::<u64>::new()
        .capacity(16)
        .consumer(a)
        .consumer(b)
        .consumer(c)
        .consumer_after([a.name(), b.name(), c.name()], d)
        .build(BusySpin)
        .expect("diamond dag should validate");
    let _producer = disruptor.producer();
    disruptor.shutdown_or_panic();
}
