# TODO

## Performance

- [ ] **Pin threads to cores.** Busy-spin consumers should not bounce between
  cores. Add optional core affinity for consumers and the producer through the
  builder.

- [ ] **Reduce false sharing for small events.** Ring slots are packed
  contiguously, so tiny payloads like `u64` can cause cache-line ping-pong
  between producer and consumer. Either pad small slots or document the
  expected event size.

- [ ] **Remove dynamic dispatch from the hot path.** Consumers are stored as
  `Box<dyn Consumer<T>>`, so every `on_event` call goes through a vtable. Keep
  dynamic dispatch in setup only, or spawn each processor with a concrete
  consumer type.

- [ ] **Batch publish API.** `SingleProducer::publish` claims, writes, and
  publishes one slot per call. Add `publish_batch(n, |slot, idx|)` that runs
  the wrap-point check once, fills `n` slots, and does a single
  `cursor.set(seq + n - 1)`. Amortizes the Release store and gating loop
  across a batch.

- [ ] **SPSC fast path in `SequenceBarrier`.** `wait_for` iterates a
  `Vec<Arc<Sequence>>`. The common 1-dep case (every fan-out leaf, every
  SPSC chain) is a single atomic load — specialize on `deps.len() == 1` to
  skip the Vec/iterator and let the barrier load one cursor directly.

## Testing

- [x] **Loom coverage.** Every concurrency primitive routes through
  `src/sync.rs`, which swaps to loom's mocked equivalents under
  `--features loom`. `tests/loom.rs` model-checks the real producer, event
  processor, and ring-buffer protocol (loom's `UnsafeCell` fails any
  interleaving with overlapping slot access); CI runs the models in
  `.github/workflows/loom.yml`.

- [ ] **Test with Miri.** Run the SPSC and DAG tests under Miri to check the
  unsafe ring-slot pointer accesses, initialization, aliasing, and destruction
  behavior for undefined behavior.
