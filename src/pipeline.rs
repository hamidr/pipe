//! Composable pipeline — a lazy effectful list of elements.
//!
//! `Pipe<B>` is analogous to `Stream[F, O]` in FS2.
//! `B` is the **element type** — chunking is an internal detail.
//! Operations like `map`, `and_then`, `flat_map` can change the
//! element type.

use std::future::Future;
use std::sync::Arc;

use crate::operator::Operator;
use crate::pull::{
    collect_all, fold_all, ArcSource, PipeError, PullAndThen, PullDrop, PullFilter, PullFlatMap,
    PullIterate, PullMap, PullOperator, PullPipe, PullScan, PullSource, PullTake, PullTap,
    PullUnfold, PullZip,
};


/// A lazy effectful list of `B` elements.
///
/// `Pipe` is a blueprint — it describes a pipeline of operations but
/// does not execute until a terminal (`.collect()`, `.fold()`, etc.) is
/// called. Because it is a description, `Pipe` implements [`Clone`]:
/// each clone materializes an independent operator chain.
///
/// ```ignore
/// let result = Pipe::from_iter(1..=10)
///     .map(|x| x * 2)
///     .filter(|x| *x > 10)
///     .take(3)
///     .collect().await?;
/// assert_eq!(result, vec![12, 14, 16]);
/// ```
pub struct Pipe<B: Send + 'static> {
    pub(crate) factory: Arc<dyn Fn() -> Box<dyn PullOperator<B>> + Send + Sync>,
}

impl<B: Send + 'static> Clone for Pipe<B> {
    fn clone(&self) -> Self {
        Self {
            factory: Arc::clone(&self.factory),
        }
    }
}

impl<B: Send + 'static> Pipe<B> {
    /// Internal: create a pipe from a factory closure.
    pub(crate) fn from_factory(f: impl Fn() -> Box<dyn PullOperator<B>> + Send + Sync + 'static) -> Self {
        Self {
            factory: Arc::new(f),
        }
    }

    // ══════════════════════════════════════════════════════
    // Constructors
    // ══════════════════════════════════════════════════════

    /// Create a stream from an iterator of elements.
    #[allow(clippy::should_implement_trait)]
    pub fn from_iter(items: impl IntoIterator<Item = B>) -> Self
    where
        B: Clone + Sync,
    {
        let data: Arc<[B]> = items.into_iter().collect::<Vec<B>>().into();
        Self::from_factory(move || Box::new(ArcSource::new(Arc::clone(&data))))
    }

    /// Create a stream of one element.
    pub fn once(item: B) -> Self
    where
        B: Clone + Sync,
    {
        let data: Arc<[B]> = Arc::from(vec![item]);
        Self::from_factory(move || Box::new(ArcSource::new(Arc::clone(&data))))
    }

    /// Create an empty stream.
    pub fn empty() -> Self {
        Self::from_factory(|| Box::new(PullSource::<B>::empty()))
    }

    /// Lazy generation from a seed. Produces elements until `step` returns `None`.
    pub fn unfold<S: Clone + Send + Sync + 'static>(
        seed: S,
        step: impl Fn(&mut S) -> Option<B> + Send + Sync + 'static,
    ) -> Self {
        let step: Arc<dyn Fn(&mut S) -> Option<B> + Send + Sync> = Arc::new(step);
        Self::from_factory(move || {
            let seed = seed.clone();
            let step = Arc::clone(&step);
            Box::new(PullUnfold::new(seed, move |s: &mut S| step(s)))
        })
    }

    /// Infinite stream: `init, f(init), f(f(init)), ...`
    pub fn iterate(init: B, step: impl Fn(&B) -> B + Send + Sync + 'static) -> Self
    where
        B: Clone + Sync,
    {
        let step: Arc<dyn Fn(&B) -> B + Send + Sync> = Arc::new(step);
        Self::from_factory(move || {
            let init = init.clone();
            let step = Arc::clone(&step);
            Box::new(PullIterate::new(init, move |b: &B| step(b)))
        })
    }

    /// Create a stream from a factory that produces a [`PullOperator`].
    ///
    /// The factory is called each time the pipe is materialized (i.e.,
    /// when a terminal like `.collect()` is invoked), producing an
    /// independent operator chain. This makes the pipe cloneable.
    pub fn from_pull(
        factory: impl Fn() -> Box<dyn PullOperator<B>> + Send + Sync + 'static,
    ) -> Self {
        Self::from_factory(factory)
    }

    /// Create a stream from a single [`PullOperator`] instance.
    ///
    /// The resulting pipe can be cloned, but only one clone may be
    /// materialized (executed). Panics if a second clone is executed.
    pub fn from_pull_once(op: impl PullOperator<B> + 'static) -> Self {
        let slot: Arc<std::sync::Mutex<Option<Box<dyn PullOperator<B>>>>> =
            Arc::new(std::sync::Mutex::new(Some(Box::new(op))));
        Self::from_factory(move || {
            slot.lock()
                .unwrap()
                .take()
                .expect("from_pull_once: operator already consumed (Pipe was cloned and both materialized)")
        })
    }

    // ══════════════════════════════════════════════════════
    // Type-changing operators
    // ══════════════════════════════════════════════════════

    /// Transform each element (may change type).
    pub fn map<C: Send + 'static>(self, f: impl Fn(B) -> C + Send + Sync + 'static) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        Pipe::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullMap {
                child,
                f: move |b| f(b),
            })
        })
    }

    /// Fused map + filter (may change type).
    pub fn and_then<C: Send + 'static>(
        self,
        f: impl Fn(B) -> Option<C> + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        Pipe::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullAndThen {
                child,
                f: move |b| f(b),
            })
        })
    }

    /// Each element expands into a sub-stream (may change type).
    pub fn flat_map<C: Send + 'static>(
        self,
        f: impl Fn(B) -> Pipe<C> + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        Pipe::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullFlatMap::new(child, move |b| f(b)))
        })
    }

    /// Stateful per-element transform (may change type).
    pub fn scan<C: Send + 'static, S: Clone + Send + Sync + 'static>(
        self,
        init: S,
        f: impl Fn(&mut S, B) -> C + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f: Arc<dyn Fn(&mut S, B) -> C + Send + Sync> = Arc::new(f);
        Pipe::from_factory(move || {
            let child = parent();
            let state = init.clone();
            let f = Arc::clone(&f);
            Box::new(PullScan {
                child,
                state,
                f: move |s: &mut S, b: B| f(s, b),
            })
        })
    }

    /// Apply an async [`Operator`] to each element (may change type).
    pub fn pipe<C: Send + 'static, T: Operator<B, C> + Sync + 'static>(self, op: T) -> Pipe<C> {
        let parent = self.factory;
        let op: Arc<dyn Operator<B, C> + Send + Sync> = Arc::new(op);
        Pipe::from_factory(move || {
            let child = parent();
            Box::new(PullPipe {
                operator: Arc::clone(&op),
                child,
            })
        })
    }

    // ══════════════════════════════════════════════════════
    // Async element operations (eval_*)
    // ══════════════════════════════════════════════════════

    /// Async per-element transform (like `map` but with `Future`).
    pub fn eval_map<C: Send + 'static, Fut: Future<Output = Result<C, PipeError>> + Send + 'static>(
        self,
        f: impl Fn(B) -> Fut + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        Pipe::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullEvalMap {
                child,
                f: move |b| f(b),
            })
        })
    }

    /// Async per-element filter.
    pub fn eval_filter<Fut: Future<Output = Result<bool, PipeError>> + Send + 'static>(
        self,
        f: impl Fn(&B) -> Fut + Send + Sync + 'static,
    ) -> Self {
        let parent = self.factory;
        let f = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullEvalFilter {
                child,
                f: move |b: &B| f(b),
            })
        })
    }

    /// Async side-effect on each element, pass through unchanged.
    ///
    /// Requires `B: Sync` because elements are borrowed across await points.
    pub fn eval_tap<Fut: Future<Output = Result<(), PipeError>> + Send + 'static>(
        self,
        f: impl Fn(&B) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        B: Sync,
    {
        let parent = self.factory;
        let f = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullEvalTap {
                child,
                f: move |b: &B| f(b),
            })
        })
    }

    // ══════════════════════════════════════════════════════
    // Same-type operators
    // ══════════════════════════════════════════════════════

    /// Keep elements matching the predicate.
    pub fn filter(self, f: impl Fn(&B) -> bool + Send + Sync + 'static) -> Self {
        let parent = self.factory;
        let f: Arc<dyn Fn(&B) -> bool + Send + Sync> = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullFilter {
                child,
                f: move |b: &B| f(b),
            })
        })
    }

    /// Side-effect on each element, pass through unchanged.
    pub fn tap(self, f: impl Fn(&B) + Send + Sync + 'static) -> Self {
        let parent = self.factory;
        let f: Arc<dyn Fn(&B) + Send + Sync> = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullTap {
                child,
                f: move |b: &B| f(b),
            })
        })
    }

    /// Alias for [`tap`](Self::tap) — follows Rust iterator convention.
    pub fn inspect(self, f: impl Fn(&B) + Send + Sync + 'static) -> Self {
        self.tap(f)
    }

    /// Take the first `n` elements.
    pub fn take(self, n: usize) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            Box::new(PullTake {
                child: parent(),
                remaining: n,
            })
        })
    }

    /// Skip the first `n` elements.
    pub fn skip(self, n: usize) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            Box::new(PullDrop {
                child: parent(),
                remaining: n,
            })
        })
    }

    /// Take elements while predicate holds, then stop.
    pub fn take_while(self, f: impl Fn(&B) -> bool + Send + Sync + 'static) -> Self {
        let parent = self.factory;
        let f: Arc<dyn Fn(&B) -> bool + Send + Sync> = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullTakeWhile {
                child,
                f: move |b: &B| f(b),
                done: false,
            })
        })
    }

    /// Skip elements while predicate holds, then pass through.
    pub fn skip_while(self, f: impl Fn(&B) -> bool + Send + Sync + 'static) -> Self {
        let parent = self.factory;
        let f: Arc<dyn Fn(&B) -> bool + Send + Sync> = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullSkipWhile {
                child,
                f: move |b: &B| f(b),
                skipping: true,
            })
        })
    }

    /// Group elements into fixed-size chunks, exposing batch boundaries.
    pub fn chunks(self, size: usize) -> Pipe<Vec<B>> {
        let parent = self.factory;
        Pipe::from_factory(move || {
            Box::new(PullChunks {
                child: parent(),
                size,
            })
        })
    }

    // ══════════════════════════════════════════════════════
    // Stream composition
    // ══════════════════════════════════════════════════════

    /// Apply a stream-level transform (FS2 `through`).
    ///
    /// The function receives this pipe and returns a new pipe.
    /// This is the primary composition mechanism for reusable transforms.
    ///
    /// ```ignore
    /// fn deduplicate(pipe: Pipe<i64>) -> Pipe<i64> {
    ///     pipe.scan(None, |prev, x| {
    ///         if *prev == Some(x) { None } else { *prev = Some(x); Some(x) }
    ///     }).and_then(|x| x)
    /// }
    /// let result = Pipe::from_iter(vec![1,1,2,2,3]).through(deduplicate).collect().await?;
    /// ```
    pub fn through<C: Send + 'static>(self, f: impl FnOnce(Pipe<B>) -> Pipe<C>) -> Pipe<C> {
        f(self)
    }

    /// Sequential composition: all elements of `self`, then all elements of `other`.
    pub fn chain(self, other: Pipe<B>) -> Self {
        let first = self.factory;
        let second = other.factory;
        Self::from_factory(move || {
            Box::new(PullChain {
                first: Some(first()),
                second: Some(second()),
            })
        })
    }

    /// Pair each element with its index: `(0, a), (1, b), (2, c), ...`
    pub fn enumerate(self) -> Pipe<(usize, B)> {
        self.scan(0usize, |idx, item| {
            let i = *idx;
            *idx += 1;
            (i, item)
        })
    }

    // ══════════════════════════════════════════════════════
    // Resource safety
    // ══════════════════════════════════════════════════════

    /// Acquire a resource, build a pipe from it, and guarantee cleanup.
    ///
    /// `acquire` runs lazily on first pull. `use_resource` builds a
    /// pipe from the acquired resource. `release` runs when the pipe
    /// completes, errors, or is dropped mid-stream.
    ///
    /// The resource is wrapped in `Arc` so both `use_resource` and
    /// `release` can access it.
    ///
    /// ```ignore
    /// Pipe::bracket(
    ///     || Box::pin(async { Ok(File::open("data.txt").await?) }),
    ///     |file| Pipe::from_reader(file),
    ///     |_file| { /* cleanup */ },
    /// )
    /// ```
    pub fn bracket<R: Send + Sync + 'static>(
        acquire: impl Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, PipeError>> + Send>>
            + Send
            + Sync
            + 'static,
        use_resource: impl Fn(R) -> Pipe<B> + Send + Sync + 'static,
        release: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        let acquire = Arc::new(acquire);
        let use_resource = Arc::new(use_resource);
        let release = Arc::new(release);
        Self::from_factory(move || {
            let acquire = Arc::clone(&acquire);
            let use_resource = Arc::clone(&use_resource);
            let release = Arc::clone(&release);
            Box::new(PullBracket {
                state: BracketState::Pending { acquire, use_resource, release },
            })
        })
    }

    // ══════════════════════════════════════════════════════
    // Error recovery
    // ══════════════════════════════════════════════════════

    /// On error, switch to a fallback stream produced by `f`.
    pub fn handle_error_with(
        self,
        f: impl Fn(PipeError) -> Pipe<B> + Send + Sync + 'static,
    ) -> Self {
        let parent = self.factory;
        let f = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullHandleError {
                child,
                handler: Box::new(move |e| f(e)),
                fallback: None,
            })
        })
    }

    /// Retry the entire stream up to `n` times on error.
    ///
    /// On error, reconstructs the stream from scratch and retries.
    /// Requires a factory function that produces the stream.
    pub fn retry(
        factory: impl Fn() -> Pipe<B> + Send + Sync + 'static,
        max_retries: usize,
    ) -> Self {
        let factory = Arc::new(factory);
        Self::from_factory(move || {
            let factory = Arc::clone(&factory);
            Box::new(PullRetry {
                factory: Box::new(move || factory()),
                current: None,
                remaining: max_retries + 1,
            })
        })
    }

    // ══════════════════════════════════════════════════════
    // Concurrency
    // ══════════════════════════════════════════════════════

    /// Buffer up to `n` chunks ahead of the consumer.
    ///
    /// Spawns the upstream on a background task at materialization time.
    /// Backpressure: the task awaits when the buffer is full.
    /// Task is cancelled when Pipe is dropped.
    pub fn prefetch(self, n: usize) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            let (tx, rx) = crate::channel::bounded::<B>(n);
            let mut root = parent();
            let handle = tokio::spawn(async move {
                while let Ok(Some(chunk)) = root.next_chunk().await {
                    if tx.send(chunk).await.is_err() {
                        break;
                    }
                }
            });
            Box::new(rx.with_abort(handle.abort_handle()))
        })
    }

    /// Merge multiple pipes into one, interleaving in arrival order.
    ///
    /// Each source runs concurrently. Backpressure via bounded buffers.
    /// Background tasks are cancelled when the merged Pipe is dropped.
    pub fn merge(pipes: Vec<Pipe<B>>) -> Self {
        if pipes.is_empty() {
            return Pipe::empty();
        }
        if pipes.len() == 1 {
            return pipes.into_iter().next().unwrap();
        }

        let factories: Vec<_> = pipes.into_iter().map(|p| p.factory).collect();
        Self::from_factory(move || {
            let (merged_tx, merged_rx) = crate::channel::bounded::<B>(factories.len());
            let mut handles = Vec::with_capacity(factories.len());

            for factory in &factories {
                let tx = merged_tx.clone();
                let mut root = factory();
                let h = tokio::spawn(async move {
                    while let Ok(Some(chunk)) = root.next_chunk().await {
                        if tx.send(chunk).await.is_err() {
                            break;
                        }
                    }
                });
                handles.push(h.abort_handle());
            }
            drop(merged_tx);

            Box::new(GuardedPull {
                inner: Box::new(merged_rx),
                _guards: handles,
            })
        })
    }

    /// Merge this pipe with another, interleaving in arrival order.
    ///
    /// Shorthand for `Pipe::merge(vec![self, other])`.
    pub fn merge_with(self, other: Pipe<B>) -> Self {
        Pipe::merge(vec![self, other])
    }

    /// Fan-out: clone each element to `n` branches.
    ///
    /// Each branch has a bounded buffer. The source blocks if ANY branch
    /// is full. Background task cancelled when ALL branches are dropped.
    ///
    /// Materialization is lazy — the source is not started until the
    /// first branch is consumed.
    pub fn broadcast(self, n: usize, buffer_size: usize) -> Vec<Pipe<B>>
    where
        B: Clone + Sync,
    {
        if n == 0 {
            return vec![];
        }
        if n == 1 {
            return vec![self];
        }

        // Shared state: initialized once when any branch first materializes.
        // Each branch gets a pre-assigned index and takes its receiver.
        let shared = Arc::new(LazyFanOut::new(n, buffer_size, self.factory));

        (0..n)
            .map(|idx| {
                let shared = Arc::clone(&shared);
                Pipe::from_factory(move || {
                    let rx = shared.take_broadcast_receiver(idx);
                    Box::new(BroadcastReceiver {
                        rx,
                        abort: Some(shared.abort_handle()),
                    })
                })
            })
            .collect()
    }

    /// Pair elements positionally with another pipe.
    ///
    /// Buffers leftover elements across chunk boundaries so no data is
    /// lost when left and right chunks have different sizes.
    pub fn zip<C: Send + 'static>(self, other: Pipe<C>) -> Pipe<(B, C)> {
        let left = self.factory;
        let right = other.factory;
        Pipe::from_factory(move || Box::new(PullZip::new(left(), right())))
    }

    /// Hash-partition elements across N branches.
    ///
    /// Each element goes to exactly one branch: `key(&element) % n`.
    /// No cloning — elements are moved. Each branch has a bounded buffer
    /// of `buffer_size` chunks. Backpressure: source blocks if any branch
    /// is full. Background task cancelled when all branches are dropped.
    ///
    /// Materialization is lazy — the source is not started until the
    /// first branch is consumed.
    ///
    /// ```ignore
    /// let partitions = pipe.partition(4, 2, |x| *x as u64);
    /// // partitions[0] gets elements where key % 4 == 0
    /// // partitions[1] gets elements where key % 4 == 1
    /// // etc.
    /// ```
    pub fn partition(
        self,
        n: usize,
        buffer_size: usize,
        key: impl Fn(&B) -> u64 + Send + Sync + 'static,
    ) -> Vec<Pipe<B>> {
        if n == 0 {
            return vec![];
        }
        if n == 1 {
            return vec![self];
        }

        let shared = Arc::new(LazyPartition::new(n, buffer_size, self.factory, key));

        (0..n)
            .map(|idx| {
                let shared = Arc::clone(&shared);
                Pipe::from_factory(move || {
                    let rx = shared.take_receiver(idx);
                    Box::new(rx)
                })
            })
            .collect()
    }

    // ══════════════════════════════════════════════════════
    // Internal
    // ══════════════════════════════════════════════════════

    /// Materialize the pipeline into a pull operator chain.
    pub(crate) fn into_pull(self) -> Box<dyn PullOperator<B>> {
        (self.factory)()
    }

    // ══════════════════════════════════════════════════════
    // Terminals
    // ══════════════════════════════════════════════════════

    /// Collect all elements into a `Vec`.
    pub async fn collect(self) -> Result<Vec<B>, PipeError> {
        let mut root = (self.factory)();
        collect_all(&mut *root).await
    }

    /// Reduce all elements to a single value.
    pub async fn fold<C: Send + 'static>(
        self,
        init: C,
        f: impl Fn(C, B) -> C + Send,
    ) -> Result<C, PipeError> {
        let mut root = (self.factory)();
        fold_all(&mut *root, init, f).await
    }

    /// Count elements.
    pub async fn count(self) -> Result<usize, PipeError> {
        let mut root = (self.factory)();
        let mut n = 0usize;
        while let Some(chunk) = root.next_chunk().await? {
            n += chunk.len();
        }
        Ok(n)
    }

    /// Run a side-effect for each element, discarding the values.
    pub async fn for_each(self, f: impl Fn(B) + Send) -> Result<(), PipeError> {
        let mut root = (self.factory)();
        while let Some(chunk) = root.next_chunk().await? {
            for item in chunk {
                f(item);
            }
        }
        Ok(())
    }

    /// Return the first element, or `None` if empty.
    pub async fn first(self) -> Result<Option<B>, PipeError> {
        let mut root = (self.factory)();
        match root.next_chunk().await? {
            Some(chunk) => Ok(chunk.into_iter().next()),
            None => Ok(None),
        }
    }

    /// Return the last element, or `None` if empty.
    pub async fn last(self) -> Result<Option<B>, PipeError> {
        let mut root = (self.factory)();
        let mut last = None;
        while let Some(chunk) = root.next_chunk().await? {
            if let Some(item) = chunk.into_iter().last() {
                last = Some(item);
            }
        }
        Ok(last)
    }
}

/// Flatten a `Pipe<Vec<B>>` back to `Pipe<B>`.
impl<B: Clone + Send + Sync + 'static> Pipe<Vec<B>> {
    /// Flatten chunked elements back to individual elements.
    pub fn unchunks(self) -> Pipe<B> {
        self.flat_map(Pipe::from_iter)
    }
}

// ══════════════════════════════════════════════════════
// Internal pull operators for new features
// ══════════════════════════════════════════════════════

use crate::pull::ChunkFut;

// ── Bracket (resource safety) ───────────────────────

enum BracketState<B: Send + 'static, R: Send + 'static> {
    Pending {
        acquire: Arc<
            dyn Fn() -> std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<R, PipeError>> + Send>,
                > + Send
                + Sync,
        >,
        use_resource: Arc<dyn Fn(R) -> Pipe<B> + Send + Sync>,
        release: Arc<dyn Fn() + Send + Sync>,
    },
    Active {
        inner: Box<dyn PullOperator<B>>,
        release: Arc<dyn Fn() + Send + Sync>,
    },
    Done,
}

struct PullBracket<B: Send + 'static, R: Send + 'static> {
    state: BracketState<B, R>,
}

impl<B: Send + 'static, R: Send + 'static> Drop for PullBracket<B, R> {
    fn drop(&mut self) {
        let old = std::mem::replace(&mut self.state, BracketState::Done);
        if let BracketState::Active { release, .. } = old {
            release();
        } else if let BracketState::Pending { release, .. } = old {
            // Acquired but never pulled — still release
            // (only if acquire was never called, release is a no-op on the resource)
            // Actually for Pending, acquire hasn't run yet so nothing to release.
            let _ = release;
        }
    }
}

impl<B: Send + 'static, R: Send + Sync + 'static> PullOperator<B> for PullBracket<B, R> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            // Lazy acquisition: acquire on first pull
            if let BracketState::Pending { .. } = &self.state {
                let old = std::mem::replace(&mut self.state, BracketState::Done);
                if let BracketState::Pending {
                    acquire,
                    use_resource,
                    release,
                } = old
                {
                    let resource = acquire().await?;
                    let pipe = use_resource(resource);
                    let inner = pipe.into_pull();
                    self.state = BracketState::Active { inner, release };
                }
            }

            match &mut self.state {
                BracketState::Active { inner, release } => match inner.next_chunk().await {
                    Ok(Some(chunk)) => Ok(Some(chunk)),
                    Ok(None) => {
                        let release = Arc::clone(release);
                        self.state = BracketState::Done;
                        release();
                        Ok(None)
                    }
                    Err(e) => {
                        let release = Arc::clone(release);
                        self.state = BracketState::Done;
                        release();
                        Err(e)
                    }
                },
                _ => Ok(None),
            }
        })
    }
}

// ── Chain (sequential composition) ──────────────────

struct PullChain<B: Send + 'static> {
    first: Option<Box<dyn PullOperator<B>>>,
    second: Option<Box<dyn PullOperator<B>>>,
}

impl<B: Send + 'static> PullOperator<B> for PullChain<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            if let Some(ref mut first) = self.first {
                match first.next_chunk().await? {
                    Some(chunk) => return Ok(Some(chunk)),
                    None => {
                        self.first = None;
                    }
                }
            }
            if let Some(ref mut second) = self.second {
                match second.next_chunk().await? {
                    Some(chunk) => return Ok(Some(chunk)),
                    None => {
                        self.second = None;
                    }
                }
            }
            Ok(None)
        })
    }
}

// ── LazyFanOut (deferred broadcast setup) ───────────

/// Shared state for lazy broadcast. Initialized on first branch access.
struct LazyFanOut<B: Send + 'static> {
    n: usize,
    buffer_size: usize,
    source_factory: Arc<dyn Fn() -> Box<dyn PullOperator<B>> + Send + Sync>,
    state: std::sync::OnceLock<LazyFanOutState<B>>,
}

struct LazyFanOutState<B> {
    receivers: std::sync::Mutex<Vec<Option<tokio::sync::mpsc::Receiver<Arc<Vec<B>>>>>>,
    abort: tokio::task::AbortHandle,
}

impl<B: Clone + Send + Sync + 'static> LazyFanOut<B> {
    fn new(
        n: usize,
        buffer_size: usize,
        source_factory: Arc<dyn Fn() -> Box<dyn PullOperator<B>> + Send + Sync>,
    ) -> Self {
        Self {
            n,
            buffer_size,
            source_factory,
            state: std::sync::OnceLock::new(),
        }
    }

    fn ensure_init(&self) -> &LazyFanOutState<B> {
        self.state.get_or_init(|| {
            let mut senders = Vec::with_capacity(self.n);
            let mut receivers = Vec::with_capacity(self.n);
            for _ in 0..self.n {
                let (tx, rx) =
                    tokio::sync::mpsc::channel::<Arc<Vec<B>>>(self.buffer_size.max(1));
                senders.push(tx);
                receivers.push(Some(rx));
            }

            let mut root = (self.source_factory)();
            let handle = tokio::spawn(async move {
                while let Ok(Some(chunk)) = root.next_chunk().await {
                    let shared: Arc<Vec<B>> = Arc::new(chunk);
                    for tx in &senders {
                        if tx.send(Arc::clone(&shared)).await.is_err() {
                            return;
                        }
                    }
                }
            });

            LazyFanOutState {
                receivers: std::sync::Mutex::new(receivers),
                abort: handle.abort_handle(),
            }
        })
    }

    fn take_broadcast_receiver(
        &self,
        idx: usize,
    ) -> tokio::sync::mpsc::Receiver<Arc<Vec<B>>> {
        let state = self.ensure_init();
        state.receivers.lock().unwrap()[idx]
            .take()
            .expect("broadcast branch already consumed")
    }

    fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.ensure_init().abort.clone()
    }
}

// ── LazyPartition (deferred partition setup) ────────

/// Shared state for lazy partition. Initialized on first branch access.
struct LazyPartition<B: Send + 'static> {
    n: usize,
    buffer_size: usize,
    source_factory: Arc<dyn Fn() -> Box<dyn PullOperator<B>> + Send + Sync>,
    key: Arc<dyn Fn(&B) -> u64 + Send + Sync>,
    state: std::sync::OnceLock<LazyPartitionState<B>>,
}

struct LazyPartitionState<B> {
    receivers: std::sync::Mutex<Vec<Option<crate::channel::Receiver<B>>>>,
}

impl<B: Send + 'static> LazyPartition<B> {
    fn new(
        n: usize,
        buffer_size: usize,
        source_factory: Arc<dyn Fn() -> Box<dyn PullOperator<B>> + Send + Sync>,
        key: impl Fn(&B) -> u64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            n,
            buffer_size,
            source_factory,
            key: Arc::new(key),
            state: std::sync::OnceLock::new(),
        }
    }

    fn ensure_init(&self) -> &LazyPartitionState<B> {
        self.state.get_or_init(|| {
            let n = self.n;
            let mut senders = Vec::with_capacity(n);
            let mut receivers = Vec::with_capacity(n);
            for _ in 0..n {
                let (tx, rx) = crate::channel::bounded::<B>(self.buffer_size);
                senders.push(tx);
                receivers.push(rx);
            }

            let mut root = (self.source_factory)();
            let key = Arc::clone(&self.key);
            let handle = tokio::spawn(async move {
                let mut buckets: Vec<Vec<B>> = (0..n).map(|_| Vec::new()).collect();
                while let Ok(Some(chunk)) = root.next_chunk().await {
                    for item in chunk {
                        let idx = key(&item) as usize % n;
                        buckets[idx].push(item);
                    }
                    for (i, bucket) in buckets.iter_mut().enumerate() {
                        if !bucket.is_empty() {
                            let batch = std::mem::take(bucket);
                            if senders[i].send(batch).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });

            let abort = handle.abort_handle();
            let receivers: Vec<_> = receivers
                .into_iter()
                .map(|rx| Some(rx.with_abort(abort.clone())))
                .collect();

            LazyPartitionState {
                receivers: std::sync::Mutex::new(receivers),
            }
        })
    }

    fn take_receiver(&self, idx: usize) -> crate::channel::Receiver<B> {
        let state = self.ensure_init();
        state.receivers.lock().unwrap()[idx]
            .take()
            .expect("partition branch already consumed")
    }
}

// ── GuardedPull (cancellation on drop) ───────────────

/// Wraps a PullOperator with abort handles that cancel background tasks on drop.
struct GuardedPull<B: Send + 'static> {
    inner: Box<dyn PullOperator<B>>,
    _guards: Vec<tokio::task::AbortHandle>,
}

impl<B: Send + 'static> Drop for GuardedPull<B> {
    fn drop(&mut self) {
        for handle in &self._guards {
            handle.abort();
        }
    }
}

impl<B: Send + 'static> PullOperator<B> for GuardedPull<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        self.inner.next_chunk()
    }
}

// ── BroadcastReceiver (Arc-based fan-out consumer) ──

/// Receives `Arc<Vec<B>>` chunks from broadcast and clones elements on pull.
struct BroadcastReceiver<B> {
    rx: tokio::sync::mpsc::Receiver<std::sync::Arc<Vec<B>>>,
    abort: Option<tokio::task::AbortHandle>,
}

impl<B> Drop for BroadcastReceiver<B> {
    fn drop(&mut self) {
        if let Some(handle) = self.abort.take() {
            handle.abort();
        }
    }
}

impl<B: Clone + Send + Sync + 'static> PullOperator<B> for BroadcastReceiver<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.rx.recv().await {
                Some(arc) => Ok(Some(arc.iter().cloned().collect())),
                None => Ok(None),
            }
        })
    }
}

// ── TakeWhile ────────────────────────────────────────

struct PullTakeWhile<B, F> {
    child: Box<dyn PullOperator<B>>,
    f: F,
    done: bool,
}

impl<B: Send + 'static, F: Fn(&B) -> bool + Send + 'static> PullOperator<B>
    for PullTakeWhile<B, F>
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            if self.done {
                return Ok(None);
            }
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    let mut taken = Vec::new();
                    for item in chunk {
                        if (self.f)(&item) {
                            taken.push(item);
                        } else {
                            self.done = true;
                            break;
                        }
                    }
                    if taken.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(taken))
                    }
                }
                None => Ok(None),
            }
        })
    }
}

// ── SkipWhile ────────────────────────────────────────

struct PullSkipWhile<B, F> {
    child: Box<dyn PullOperator<B>>,
    f: F,
    skipping: bool,
}

impl<B: Send + 'static, F: Fn(&B) -> bool + Send + 'static> PullOperator<B>
    for PullSkipWhile<B, F>
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            loop {
                match self.child.next_chunk().await? {
                    Some(chunk) => {
                        if !self.skipping {
                            return Ok(Some(chunk));
                        }
                        let mut rest = Vec::new();
                        for item in chunk {
                            if self.skipping {
                                if !(self.f)(&item) {
                                    self.skipping = false;
                                    rest.push(item);
                                }
                            } else {
                                rest.push(item);
                            }
                        }
                        if !rest.is_empty() {
                            return Ok(Some(rest));
                        }
                    }
                    None => return Ok(None),
                }
            }
        })
    }
}

// ── Chunks ───────────────────────────────────────────

struct PullChunks<B> {
    child: Box<dyn PullOperator<B>>,
    size: usize,
}

impl<B: Send + 'static> PullOperator<Vec<B>> for PullChunks<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, Vec<B>> {
        Box::pin(async move {
            let mut buffer = Vec::with_capacity(self.size);
            while buffer.len() < self.size {
                match self.child.next_chunk().await? {
                    Some(chunk) => buffer.extend(chunk),
                    None => break,
                }
            }
            if buffer.is_empty() {
                return Ok(None);
            }
            // Split buffer into sized groups using drain to avoid re-iteration
            let num_groups = (buffer.len() + self.size - 1) / self.size;
            let mut groups = Vec::with_capacity(num_groups);
            while buffer.len() > self.size {
                let rest = buffer.split_off(self.size);
                groups.push(std::mem::replace(&mut buffer, rest));
            }
            if !buffer.is_empty() {
                groups.push(buffer);
            }
            Ok(Some(groups))
        })
    }
}

// ── EvalMap (async per-element transform) ────────────

struct PullEvalMap<B, F> {
    child: Box<dyn PullOperator<B>>,
    f: F,
}

impl<A: Send + 'static, B: Send + 'static, Fut, F> PullOperator<B> for PullEvalMap<A, F>
where
    Fut: Future<Output = Result<B, PipeError>> + Send,
    F: Fn(A) -> Fut + Send + 'static,
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    let mut result = Vec::with_capacity(chunk.len());
                    for item in chunk {
                        result.push((self.f)(item).await?);
                    }
                    Ok(Some(result))
                }
                None => Ok(None),
            }
        })
    }
}

// ── EvalFilter (async predicate) ─────────────────────

struct PullEvalFilter<B, F> {
    child: Box<dyn PullOperator<B>>,
    f: F,
}

impl<B: Send + 'static, Fut, F> PullOperator<B> for PullEvalFilter<B, F>
where
    Fut: Future<Output = Result<bool, PipeError>> + Send,
    F: Fn(&B) -> Fut + Send + 'static,
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            loop {
                match self.child.next_chunk().await? {
                    Some(chunk) => {
                        let mut filtered = Vec::new();
                        for item in chunk {
                            if (self.f)(&item).await? {
                                filtered.push(item);
                            }
                        }
                        if !filtered.is_empty() {
                            return Ok(Some(filtered));
                        }
                    }
                    None => return Ok(None),
                }
            }
        })
    }
}

// ── EvalTap (async side-effect) ──────────────────────

struct PullEvalTap<B, F> {
    child: Box<dyn PullOperator<B>>,
    f: F,
}

impl<B: Send + Sync + 'static, Fut, F> PullOperator<B> for PullEvalTap<B, F>
where
    Fut: Future<Output = Result<(), PipeError>> + Send,
    F: Fn(&B) -> Fut + Send + 'static,
{
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.child.next_chunk().await? {
                Some(chunk) => {
                    for item in &chunk {
                        (self.f)(item).await?;
                    }
                    Ok(Some(chunk))
                }
                None => Ok(None),
            }
        })
    }
}

// ── HandleError ──────────────────────────────────────

struct PullHandleError<B: Send + 'static> {
    child: Box<dyn PullOperator<B>>,
    handler: Box<dyn Fn(PipeError) -> Pipe<B> + Send>,
    fallback: Option<Box<dyn PullOperator<B>>>,
}

impl<B: Send + 'static> PullOperator<B> for PullHandleError<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            // If we're in fallback mode, drain the fallback pipe
            if let Some(ref mut fb) = self.fallback {
                return fb.next_chunk().await;
            }
            match self.child.next_chunk().await {
                Ok(chunk) => Ok(chunk),
                Err(e) => {
                    // Switch to fallback stream
                    let pipe = (self.handler)(e);
                    self.fallback = Some(pipe.into_pull());
                    // Pull first chunk from fallback
                    self.fallback.as_mut().unwrap().next_chunk().await
                }
            }
        })
    }
}

// ── Retry ────────────────────────────────────────────

struct PullRetry<B: Send + 'static> {
    factory: Box<dyn Fn() -> Pipe<B> + Send>,
    current: Option<Box<dyn PullOperator<B>>>,
    remaining: usize,
}

impl<B: Send + 'static> PullOperator<B> for PullRetry<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            loop {
                if self.current.is_none() {
                    if self.remaining == 0 {
                        return Err(PipeError::RetryExhausted);
                    }
                    self.remaining -= 1;
                    self.current = Some((self.factory)().into_pull());
                }
                match self.current.as_mut().unwrap().next_chunk().await {
                    Ok(chunk) => return Ok(chunk),
                    Err(_e) if self.remaining > 0 => {
                        self.current = None; // retry
                    }
                    Err(e) => return Err(e),
                }
            }
        })
    }
}

// ══════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::PinFut;

    // ── Same-type operations ──────────────────────────

    #[tokio::test]
    async fn from_iter_collect() {
        let result = Pipe::from_iter(vec![1, 2, 3]).collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn once_single_element() {
        let result = Pipe::once(42).collect().await.unwrap();
        assert_eq!(result, vec![42]);
    }

    #[tokio::test]
    async fn empty_stream() {
        let result = Pipe::<i64>::empty().collect().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn filter_evens() {
        let result = Pipe::from_iter(1..=6)
            .filter(|x| x % 2 == 0)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![2, 4, 6]);
    }

    #[tokio::test]
    async fn take_first_three() {
        let result = Pipe::from_iter(1..=10).take(3).collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn skip_first_two() {
        let result = Pipe::from_iter(1..=5).skip(2).collect().await.unwrap();
        assert_eq!(result, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn fold_sum() {
        let sum = Pipe::from_iter(1..=5)
            .fold(0i64, |acc, x| acc + x)
            .await
            .unwrap();
        assert_eq!(sum, 15);
    }

    #[tokio::test]
    async fn count_elements() {
        let n = Pipe::from_iter(1..=7).count().await.unwrap();
        assert_eq!(n, 7);
    }

    #[tokio::test]
    async fn tap_observes() {
        use std::sync::{Arc, Mutex};
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .tap(move |x| seen2.lock().unwrap().push(*x))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn chain_map_filter_take() {
        let result = Pipe::from_iter(1..=10)
            .map(|x| x * 2)
            .filter(|x| *x > 10)
            .take(3)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![12, 14, 16]);
    }

    // ── Type-changing operations ──────────────────────

    #[tokio::test]
    async fn map_changes_type() {
        let result: Vec<String> = Pipe::from_iter(vec![1, 2, 3])
            .map(|x: i64| x.to_string())
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn and_then_changes_type() {
        let result: Vec<String> = Pipe::from_iter(1..=5)
            .and_then(|x: i64| if x > 2 { Some(format!("v{x}")) } else { None })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["v3", "v4", "v5"]);
    }

    #[tokio::test]
    async fn flat_map_changes_type() {
        let result: Vec<String> = Pipe::from_iter(vec![1, 2])
            .flat_map(|x: i64| Pipe::from_iter(vec![format!("{x}a"), format!("{x}b")]))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["1a", "1b", "2a", "2b"]);
    }

    #[tokio::test]
    async fn scan_changes_type() {
        let result: Vec<String> = Pipe::from_iter(vec![1, 2, 3])
            .scan(0i64, |acc, x: i64| {
                *acc += x;
                format!("sum={acc}")
            })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["sum=1", "sum=3", "sum=6"]);
    }

    #[derive(Debug)]
    struct ToStringOp;

    impl Operator<i64, String> for ToStringOp {
        fn execute<'a>(&'a self, input: i64) -> PinFut<'a, String> {
            Box::pin(async move { Ok(format!("n{input}")) })
        }
    }

    #[tokio::test]
    async fn pipe_changes_type() {
        let result: Vec<String> = Pipe::from_iter(vec![1, 2, 3])
            .pipe(ToStringOp)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["n1", "n2", "n3"]);
    }

    #[tokio::test]
    async fn multi_hop_type_change() {
        let result: Vec<usize> = Pipe::from_iter(1..=3)
            .map(|x: i64| x.to_string())
            .map(|s| s.len())
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 1, 1]);
    }

    #[tokio::test]
    async fn type_change_then_filter() {
        let result: Vec<String> = Pipe::from_iter(1..=10)
            .map(|x: i64| format!("item-{x}"))
            .filter(|s| s.ends_with('5'))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["item-5"]);
    }

    // ── Infinite streams ──────────────────────────────

    #[tokio::test]
    async fn iterate_naturals() {
        let result = Pipe::iterate(0, |x| x + 1).take(5).collect().await.unwrap();
        assert_eq!(result, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn unfold_fibonacci() {
        let result = Pipe::unfold((0u64, 1u64), |s| {
            let val = s.0;
            let next = s.0.checked_add(s.1)?;
            *s = (s.1, next);
            Some(val)
        })
        .take(10)
        .collect()
        .await
        .unwrap();
        assert_eq!(result, vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34]);
    }

    #[tokio::test]
    async fn iterate_with_type_change() {
        let result: Vec<String> = Pipe::iterate(1, |x| x + 1)
            .take(3)
            .map(|x: i64| format!("#{x}"))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["#1", "#2", "#3"]);
    }

    // ── flat_map ──────────────────────────────────────

    #[tokio::test]
    async fn flat_map_same_type() {
        let result = Pipe::from_iter(1..=3)
            .flat_map(|x| Pipe::from_iter(0..x))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![0, 0, 1, 0, 1, 2]);
    }

    #[tokio::test]
    async fn flat_map_infinite_sub() {
        let result = Pipe::once(1)
            .flat_map(|x| Pipe::iterate(x, |n| n + 1))
            .take(5)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    // ── Custom PullOperator ─────────────────────────────

    #[tokio::test]
    async fn from_pull_custom_source() {
        use crate::pull::{ChunkFut, PullOperator};

        struct Counter {
            current: i64,
            max: i64,
            chunk_size: usize,
        }

        impl PullOperator<i64> for Counter {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async move {
                    if self.current > self.max {
                        return Ok(None);
                    }
                    let mut chunk = Vec::with_capacity(self.chunk_size);
                    for _ in 0..self.chunk_size {
                        if self.current > self.max {
                            break;
                        }
                        chunk.push(self.current);
                        self.current += 1;
                    }
                    Ok(Some(chunk))
                })
            }
        }

        let result = Pipe::from_pull_once(Counter {
            current: 1,
            max: 5,
            chunk_size: 2,
        })
        .map(|x| x * 10)
        .collect()
        .await
        .unwrap();
        assert_eq!(result, vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test]
    async fn from_pull_with_filter_and_take() {
        use crate::pull::{ChunkFut, PullOperator};

        struct Naturals {
            n: i64,
        }

        impl PullOperator<i64> for Naturals {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async move {
                    let mut chunk = Vec::with_capacity(64);
                    for _ in 0..64 {
                        self.n += 1;
                        chunk.push(self.n);
                    }
                    Ok(Some(chunk))
                })
            }
        }

        let result = Pipe::from_pull_once(Naturals { n: 0 })
            .filter(|x| x % 3 == 0)
            .take(4)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![3, 6, 9, 12]);
    }

    // ── Concurrency ─────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prefetch_buffers_ahead() {
        let result = Pipe::from_iter(1..=5)
            .prefetch(2)
            .map(|x| x * 10)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn merge_interleaves() {
        let a = Pipe::from_iter(vec![1, 3, 5]);
        let b = Pipe::from_iter(vec![2, 4, 6]);
        let mut result = Pipe::merge(vec![a, b]).collect().await.unwrap();
        result.sort();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn merge_empty() {
        let result = Pipe::<i64>::merge(vec![]).collect().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn merge_single() {
        let result = Pipe::merge(vec![Pipe::from_iter(vec![1, 2, 3])])
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broadcast_fans_out() {
        let branches = Pipe::from_iter(vec![1, 2, 3]).broadcast(2, 2);
        assert_eq!(branches.len(), 2);

        let mut results = Vec::new();
        for branch in branches {
            results.push(branch.collect().await.unwrap());
        }
        assert_eq!(results[0], vec![1, 2, 3]);
        assert_eq!(results[1], vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn zip_pairs_positionally() {
        let a = Pipe::from_iter(vec![1, 2, 3]);
        let b = Pipe::from_iter(vec!["a", "b", "c"]);
        let result = a.zip(b).collect().await.unwrap();
        assert_eq!(result, vec![(1, "a"), (2, "b"), (3, "c")]);
    }

    #[tokio::test]
    async fn zip_unequal_length() {
        let a = Pipe::from_iter(vec![1, 2, 3, 4, 5]);
        let b = Pipe::from_iter(vec!["x", "y"]);
        let result = a.zip(b).collect().await.unwrap();
        assert_eq!(result, vec![(1, "x"), (2, "y")]);
    }

    // ── Partition ─────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partition_splits_by_key() {
        let parts = Pipe::from_iter(0..10i64).partition(2, 2, |x| *x as u64);

        assert_eq!(parts.len(), 2);
        let mut even = parts.into_iter().next().unwrap().collect().await.unwrap();
        // Partition 0 gets elements where key % 2 == 0
        even.sort();
        assert_eq!(even, vec![0, 2, 4, 6, 8]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partition_all_elements_preserved() {
        let parts = Pipe::from_iter(1..=12i64).partition(3, 2, |x| *x as u64);

        let mut all = Vec::new();
        for part in parts {
            all.extend(part.collect().await.unwrap());
        }
        all.sort();
        assert_eq!(all, (1..=12).collect::<Vec<_>>());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partition_single_returns_self() {
        let parts = Pipe::from_iter(vec![1, 2, 3]).partition(1, 2, |x| *x as u64);
        assert_eq!(parts.len(), 1);
        let result = parts.into_iter().next().unwrap().collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn partition_then_merge_roundtrip() {
        let parts = Pipe::from_iter(1..=20i64).partition(4, 2, |x| *x as u64);

        let merged = Pipe::merge(parts);
        let mut result = merged.collect().await.unwrap();
        result.sort();
        assert_eq!(result, (1..=20).collect::<Vec<_>>());
    }

    // ── New features ─────────────────────────────────────

    #[tokio::test]
    async fn take_while_stops_at_predicate() {
        let result = Pipe::from_iter(1..=10)
            .take_while(|x| *x < 5)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn skip_while_starts_at_predicate() {
        let result = Pipe::from_iter(1..=6)
            .skip_while(|x| *x < 4)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![4, 5, 6]);
    }

    #[tokio::test]
    async fn chunks_groups_elements() {
        let result = Pipe::from_iter(1..=7).chunks(3).collect().await.unwrap();
        assert_eq!(result, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]);
    }

    #[tokio::test]
    async fn unchunks_flattens() {
        let result = Pipe::from_iter(vec![vec![1, 2], vec![3, 4, 5]])
            .unchunks()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn through_composition() {
        fn double_evens(pipe: Pipe<i64>) -> Pipe<i64> {
            pipe.filter(|x| x % 2 == 0).map(|x| x * 2)
        }
        let result = Pipe::from_iter(1..=6)
            .through(double_evens)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![4, 8, 12]);
    }

    #[tokio::test]
    async fn eval_map_async_transform() {
        let result = Pipe::from_iter(vec![1, 2, 3])
            .eval_map(|x| async move { Ok(format!("async-{x}")) })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["async-1", "async-2", "async-3"]);
    }

    #[tokio::test]
    async fn eval_filter_async_predicate() {
        let result = Pipe::from_iter(1..=6)
            .eval_filter(|x| {
                let x = *x;
                async move { Ok(x % 2 == 0) }
            })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![2, 4, 6]);
    }

    #[tokio::test]
    async fn eval_tap_async_side_effect() {
        use std::sync::{Arc, Mutex};
        let log = Arc::new(Mutex::new(Vec::new()));
        let log2 = log.clone();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .eval_tap(move |x| {
                let log = log2.clone();
                let x = *x;
                async move {
                    log.lock().unwrap().push(x);
                    Ok(())
                }
            })
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
        assert_eq!(*log.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn handle_error_with_recovers() {
        use crate::pull::{ChunkFut, PullOperator};

        struct FailAfter {
            count: usize,
            yielded: usize,
        }
        impl PullOperator<i64> for FailAfter {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async move {
                    if self.yielded >= self.count {
                        return Err("boom".into());
                    }
                    self.yielded += 1;
                    Ok(Some(vec![self.yielded as i64]))
                })
            }
        }

        let result = Pipe::from_pull_once(FailAfter {
            count: 2,
            yielded: 0,
        })
        .handle_error_with(|_| Pipe::from_iter(vec![99]))
        .collect()
        .await
        .unwrap();
        assert_eq!(result, vec![1, 2, 99]);
    }

    #[tokio::test]
    async fn retry_recovers_from_failure() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let attempt = Arc::new(AtomicUsize::new(0));

        let attempt2 = attempt.clone();
        let result = Pipe::retry(
            move || {
                let n = attempt2.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    // First two attempts fail after yielding some data
                    Pipe::from_iter(vec![1, 2]).eval_map(move |x| async move {
                        if x == 2 {
                            Err("fail".into())
                        } else {
                            Ok(x)
                        }
                    })
                } else {
                    // Third attempt succeeds
                    Pipe::from_iter(vec![10, 20, 30])
                }
            },
            3,
        )
        .collect()
        .await
        .unwrap();
        assert_eq!(result, vec![10, 20, 30]);
    }

    // ── Terminals ─────────────────────────────────────────

    #[tokio::test]
    async fn for_each_runs_side_effect() {
        use std::sync::{Arc, Mutex};
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        Pipe::from_iter(vec![1, 2, 3])
            .for_each(move |x| seen2.lock().unwrap().push(x))
            .await
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn first_returns_first_element() {
        let result = Pipe::from_iter(vec![10, 20, 30]).first().await.unwrap();
        assert_eq!(result, Some(10));
    }

    #[tokio::test]
    async fn first_returns_none_for_empty() {
        let result = Pipe::<i64>::empty().first().await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn last_returns_last_element() {
        let result = Pipe::from_iter(vec![10, 20, 30]).last().await.unwrap();
        assert_eq!(result, Some(30));
    }

    #[tokio::test]
    async fn last_returns_none_for_empty() {
        let result = Pipe::<i64>::empty().last().await.unwrap();
        assert_eq!(result, None);
    }

    // ── Chain / enumerate / inspect / merge_with ─────────

    #[tokio::test]
    async fn chain_sequential() {
        let a = Pipe::from_iter(vec![1, 2]);
        let b = Pipe::from_iter(vec![3, 4, 5]);
        let result = a.chain(b).collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn chain_first_empty() {
        let result = Pipe::<i64>::empty()
            .chain(Pipe::from_iter(vec![1, 2]))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2]);
    }

    #[tokio::test]
    async fn chain_second_empty() {
        let result = Pipe::from_iter(vec![1, 2])
            .chain(Pipe::empty())
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2]);
    }

    #[tokio::test]
    async fn enumerate_pairs_with_index() {
        let result = Pipe::from_iter(vec!["a", "b", "c"])
            .enumerate()
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![(0, "a"), (1, "b"), (2, "c")]);
    }

    #[tokio::test]
    async fn inspect_observes() {
        use std::sync::{Arc, Mutex};
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .inspect(move |x| seen2.lock().unwrap().push(*x))
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn merge_with_two_pipes() {
        let a = Pipe::from_iter(vec![1, 3, 5]);
        let b = Pipe::from_iter(vec![2, 4, 6]);
        let mut result = a.merge_with(b).collect().await.unwrap();
        result.sort();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn from_pull_once_no_boxing() {
        use crate::pull::{ChunkFut, PullOperator};

        struct Trio;
        impl PullOperator<i64> for Trio {
            fn next_chunk(&mut self) -> ChunkFut<'_, i64> {
                Box::pin(async { Ok(Some(vec![1, 2, 3])) })
            }
        }

        let result = Pipe::from_pull_once(Trio).take(3).collect().await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn from_pull_factory() {
        let pipe = Pipe::from_pull(|| Box::new(crate::pull::PullSource::new(vec![1, 2, 3])));
        // Clone the pipe and collect from both
        let clone = pipe.clone();
        let a = pipe.collect().await.unwrap();
        let b = clone.collect().await.unwrap();
        assert_eq!(a, vec![1, 2, 3]);
        assert_eq!(b, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn clone_produces_independent_results() {
        let pipe = Pipe::from_iter(vec![1, 2, 3, 4, 5])
            .filter(|x| x % 2 == 0)
            .map(|x| x * 10);

        let clone = pipe.clone();

        let a = pipe.collect().await.unwrap();
        let b = clone.collect().await.unwrap();
        assert_eq!(a, vec![20, 40]);
        assert_eq!(b, vec![20, 40]);
    }

    #[tokio::test]
    async fn clone_with_scan_independent_state() {
        let pipe = Pipe::from_iter(vec![1, 2, 3])
            .scan(0i64, |acc, x| { *acc += x; *acc });

        let clone = pipe.clone();

        let a = pipe.collect().await.unwrap();
        let b = clone.collect().await.unwrap();
        // Both see independent accumulator state
        assert_eq!(a, vec![1, 3, 6]);
        assert_eq!(b, vec![1, 3, 6]);
    }

    // ── Bracket ───────────────────────────────────────────

    #[tokio::test]
    async fn bracket_releases_on_completion() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let released = Arc::new(AtomicBool::new(false));
        let released2 = released.clone();

        let result = Pipe::bracket(
            || Box::pin(async { Ok(vec![1i64, 2, 3]) }),
            |data| Pipe::from_iter(data),
            move || released2.store(true, Ordering::SeqCst),
        )
        .collect()
        .await
        .unwrap();

        assert_eq!(result, vec![1, 2, 3]);
        assert!(released.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn bracket_releases_on_error() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let released = Arc::new(AtomicBool::new(false));
        let released2 = released.clone();

        let result = Pipe::<i64>::bracket(
            || Box::pin(async { Ok(()) }),
            |_| {
                Pipe::from_iter(vec![1i64, 2])
                    .eval_map(|x| async move {
                        if x == 2 { Err("boom".into()) } else { Ok(x) }
                    })
            },
            move || released2.store(true, Ordering::SeqCst),
        )
        .collect()
        .await;

        assert!(result.is_err());
        assert!(released.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn bracket_releases_on_drop() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let released = Arc::new(AtomicBool::new(false));
        let released2 = released.clone();

        // Create a bracket pipe but only take 1 element, then drop
        let result = Pipe::bracket(
            || Box::pin(async { Ok(vec![10i64, 20, 30]) }),
            |data| Pipe::from_iter(data),
            move || released2.store(true, Ordering::SeqCst),
        )
        .take(1)
        .first()
        .await
        .unwrap();

        assert_eq!(result, Some(10));
        assert!(released.load(Ordering::SeqCst));
    }
}
