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
