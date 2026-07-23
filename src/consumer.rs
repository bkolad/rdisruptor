pub trait Consumer<T>: Send {
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    fn on_event(&mut self, event: &T, sequence: i64, end_of_batch: bool);

    fn on_start(&mut self) {}

    fn on_shutdown(&mut self) {}
}

/// Boxed type-erased consumer used by the builder so heterogeneous handlers
/// can sit in one Vec.
pub(crate) type BoxedConsumer<T> = Box<dyn Consumer<T> + 'static>;

/// A consumer that mutates events in place.
///
/// A mutable stage writes its results directly into the ring slot — no
/// interior mutability, no per-event allocation. Downstream stages observe the
/// mutation through their sequence barrier's Acquire load of this stage's
/// cursor.
///
/// Exclusivity is proven at build time: registering a mutable consumer fails
/// with [`BuildError::ConcurrentMutConsumer`](crate::BuildError) unless every
/// other stage in the graph is an ancestor or a descendant of it. Ancestors
/// have finished with a slot before this stage claims it; descendants cannot
/// touch it until this stage's cursor advances past it. A stage with an
/// incomparable sibling could run concurrently on the same slot, so such
/// graphs are rejected.
pub trait MutConsumer<T>: Send {
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    fn on_event(&mut self, event: &mut T, sequence: i64, end_of_batch: bool);

    fn on_start(&mut self) {}

    fn on_shutdown(&mut self) {}
}

pub(crate) type BoxedMutConsumer<T> = Box<dyn MutConsumer<T> + 'static>;

/// A registered stage: reads events, or mutates them in place.
pub(crate) enum Stage<T> {
    Read(BoxedConsumer<T>),
    Mut(BoxedMutConsumer<T>),
}

impl<T> Stage<T> {
    pub(crate) fn is_mut(&self) -> bool {
        matches!(self, Self::Mut(_))
    }

    pub(crate) fn on_start(&mut self) {
        match self {
            Self::Read(c) => c.on_start(),
            Self::Mut(c) => c.on_start(),
        }
    }

    pub(crate) fn on_shutdown(&mut self) {
        match self {
            Self::Read(c) => c.on_shutdown(),
            Self::Mut(c) => c.on_shutdown(),
        }
    }
}
