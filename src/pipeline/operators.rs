//! Synchronous operators that transform pipe elements.

use std::sync::Arc;

use crate::operator::Operator;
use crate::pull::{
    PullAndThen, PullDrop, PullFilter, PullFlatMap, PullMap, PullPipe, PullScan,
    PullTake, PullTap,
};

use super::pull_ops::{
    PullChain, PullChunks, PullInterleave, PullIntersperse, PullSlidingWindow, PullSkipWhile,
    PullTakeWhile,
};
use super::Pipe;

impl<B: Send + 'static> Pipe<B> {
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

    /// Alias for [`tap`](Self::tap) -- follows Rust iterator convention.
    pub fn inspect(self, f: impl Fn(&B) + Send + Sync + 'static) -> Self {
        self.tap(f)
    }

    /// Stop producing elements when the token is cancelled.
    ///
    /// The pipe returns `None` on the next pull after cancellation,
    /// allowing downstream to drain naturally instead of aborting.
    pub fn with_cancel(self, token: crate::cancel::CancelToken) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            let child = parent();
            let token = token.clone();
            Box::new(crate::cancel::PullCancel { child, token })
        })
    }

    /// Insert a monitoring point that reports element counts and errors.
    ///
    /// The callback receives a [`MeterStats`](crate::meter::MeterStats)
    /// snapshot after each chunk, on completion, and on error.
    pub fn meter_with(
        self,
        name: impl Into<String>,
        on_chunk: impl Fn(&crate::meter::MeterStats) + Send + Sync + 'static,
    ) -> Self {
        let parent = self.factory;
        let name = name.into();
        let on_chunk: std::sync::Arc<dyn Fn(&crate::meter::MeterStats) + Send + Sync> =
            std::sync::Arc::new(on_chunk);
        Self::from_factory(move || {
            let child = parent();
            let name = name.clone();
            let on_chunk = std::sync::Arc::clone(&on_chunk);
            Box::new(crate::meter::PullMeter {
                child,
                name: name.clone(),
                on_chunk: Box::new(move |s| on_chunk(s)),
                stats: crate::meter::MeterStats {
                    name,
                    elements: 0,
                    chunks: 0,
                    completed: false,
                    errored: false,
                },
            })
        })
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

    /// Emit overlapping windows of `size` elements.
    ///
    /// Each window shares `size - 1` elements with the previous one:
    /// `[1,2,3], [2,3,4], [3,4,5], ...`
    ///
    /// Requires `B: Clone` since elements appear in multiple windows.
    pub fn sliding_window(self, size: usize) -> Pipe<Vec<B>>
    where
        B: Clone + Sync,
    {
        assert!(size > 0, "sliding_window size must be > 0");
        let parent = self.factory;
        Pipe::from_factory(move || {
            Box::new(PullSlidingWindow {
                child: parent(),
                size,
                buffer: std::collections::VecDeque::with_capacity(size),
                done: false,
            })
        })
    }

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

    /// Alternate elements from two pipes in round-robin order.
    ///
    /// Deterministic, unlike [`merge`](Self::merge) which interleaves
    /// by arrival time. Stops when either pipe is exhausted.
    pub fn interleave(self, other: Pipe<B>) -> Self {
        let left = self.factory;
        let right = other.factory;
        Self::from_factory(move || {
            Box::new(PullInterleave {
                left: left(),
                right: right(),
                left_turn: true,
                left_buf: std::collections::VecDeque::new(),
                right_buf: std::collections::VecDeque::new(),
                left_done: false,
                right_done: false,
            })
        })
    }

    /// Insert a separator between every two elements.
    ///
    /// `[1, 2, 3].intersperse(0)` -> `[1, 0, 2, 0, 3]`
    pub fn intersperse(self, separator: B) -> Self
    where
        B: Clone + Sync,
    {
        let sep = Arc::new(separator);
        let parent = self.factory;
        Self::from_factory(move || {
            let sep = Arc::clone(&sep);
            Box::new(PullIntersperse {
                child: parent(),
                separator: sep,
                buffer: Vec::new(),
                first: true,
                done: false,
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

    /// Wrap each element in `Ok`, catching errors as `Err` elements.
    ///
    /// Unlike normal error propagation which stops the pipe, `attempt`
    /// converts errors into elements so the pipe continues.
    pub fn attempt(self) -> Pipe<Result<B, crate::pull::PipeError>> {
        let parent = self.factory;
        Pipe::from_factory(move || {
            Box::new(super::pull_ops::PullAttempt::new(parent()))
        })
    }

    /// Wrap each element in `Some`, then emit `None` at the end.
    ///
    /// Paired with [`un_none_terminate`](Pipe::<Option<B>>::un_none_terminate)
    /// for Option-based stream termination protocols.
    pub fn none_terminate(self) -> Pipe<Option<B>> {
        let parent = self.factory;
        Pipe::from_factory(move || {
            Box::new(super::pull_ops::PullNoneTerminate {
                child: parent(),
                done: false,
            })
        })
    }
}

/// Unwrap `Option`-wrapped elements, stopping at the first `None`.
impl<B: Send + 'static> Pipe<Option<B>> {
    pub fn un_none_terminate(self) -> Pipe<B> {
        let parent = self.factory;
        Pipe::from_factory(move || {
            Box::new(super::pull_ops::PullUnNoneTerminate {
                child: parent(),
            })
        })
    }
}
