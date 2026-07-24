use crate::sync::SlotCell;

pub(crate) struct RingBuffer<T> {
    slots: Box<[SlotCell<T>]>,
    mask: i64,
}

// Send is derived: `SlotCell<T>` is Send when T is Send. Sync needs the
// manual impl below because the inner UnsafeCell is never Sync. Consumers in
// different DAG branches may concurrently read the same slot, so sharing the
// ring across threads also requires its events to be Sync.
unsafe impl<T: Send + Sync> Sync for RingBuffer<T> {}

impl<T: Default> RingBuffer<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(
            capacity.is_power_of_two(),
            "capacity must be a power of two"
        );
        assert!(capacity >= 2, "capacity must be at least 2");
        assert!(capacity <= i64::MAX as usize, "capacity too large");

        let mut v: Vec<SlotCell<T>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            v.push(SlotCell::new(T::default()));
        }

        Self {
            slots: v.into_boxed_slice(),
            mask: (capacity as i64) - 1,
        }
    }
}

impl<T> RingBuffer<T> {
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    fn index(&self, seq: i64) -> usize {
        (seq & self.mask) as usize
    }

    /// Run `f` with a shared pointer to the slot for `seq`.
    ///
    /// Dereferencing the pointer requires that no concurrent `&mut` to the
    /// same slot exists. The Disruptor protocol enforces this: consumers hold
    /// shared read access between publish and consumer-cursor-advance. Under
    /// loom, overlapping slot access is detected regardless of what `f` does
    /// with the pointer.
    #[inline]
    pub(crate) fn with_slot<R>(&self, seq: i64, f: impl FnOnce(*const T) -> R) -> R {
        let idx = self.index(seq);
        // SAFETY: idx < capacity by mask construction
        unsafe { self.slots.get_unchecked(idx) }.with(f)
    }

    /// Run `f` with an exclusive pointer to the slot for `seq`.
    ///
    /// Dereferencing the pointer requires that no other access to the same
    /// slot is concurrent. The Disruptor protocol enforces this: the producer
    /// holds exclusive access to a slot between claim and publish.
    #[inline]
    pub(crate) fn with_slot_mut<R>(&self, seq: i64, f: impl FnOnce(*mut T) -> R) -> R {
        let idx = self.index(seq);
        // SAFETY: idx < capacity by mask construction
        unsafe { self.slots.get_unchecked(idx) }.with_mut(f)
    }
}
