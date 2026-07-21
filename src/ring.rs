use std::cell::UnsafeCell;

pub(crate) struct RingBuffer<T> {
    slots: Box<[UnsafeCell<T>]>,
    mask: i64,
}

// Send is derived: `UnsafeCell<T>` is Send when T is Send. Sync needs the
// manual impl below because UnsafeCell is never Sync. Consumers in different
// DAG branches may concurrently read the same slot, so sharing the ring across
// threads also requires its events to be Sync.
unsafe impl<T: Send + Sync> Sync for RingBuffer<T> {}

impl<T: Default> RingBuffer<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(
            capacity.is_power_of_two(),
            "capacity must be a power of two"
        );
        assert!(capacity >= 2, "capacity must be at least 2");
        assert!(capacity <= i64::MAX as usize, "capacity too large");

        let mut v: Vec<UnsafeCell<T>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            v.push(UnsafeCell::new(T::default()));
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

    /// Returns a raw pointer to the slot for `seq`.
    ///
    /// Caller must ensure no concurrent &mut to the same slot. The Disruptor
    /// protocol enforces this: producer holds exclusive access to a slot
    /// between claim and publish, consumers hold shared read access between
    /// publish and consumer-cursor-advance.
    #[inline]
    pub(crate) fn slot_ptr(&self, seq: i64) -> *mut T {
        let idx = (seq & self.mask) as usize;
        // SAFETY: idx < capacity by mask construction
        unsafe { self.slots.get_unchecked(idx).get() }
    }
}
