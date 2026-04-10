//! Error recovery operators: handle_error_with, retry, bracket.

use std::sync::Arc;

use crate::pull::PipeError;

use super::pull_ops::{BracketState, PullBracket, PullHandleError, PullOnFinalize, PullRetry};
use super::Pipe;

impl<B: Send + 'static> Pipe<B> {
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
    ///     |file| Pipe::from_reader(Arc::try_unwrap(file).unwrap()),
    ///     |file| drop(file),
    /// )
    /// ```
    pub fn bracket<R: Send + Sync + 'static>(
        acquire: impl Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, PipeError>> + Send>>
            + Send
            + Sync
            + 'static,
        use_resource: impl Fn(Arc<R>) -> Pipe<B> + Send + Sync + 'static,
        release: impl Fn(Arc<R>) + Send + Sync + 'static,
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

    /// Run a callback when this pipe completes, errors, or is dropped.
    ///
    /// Simpler than [`bracket`](Self::bracket) when you don't need
    /// acquire/release lifecycle -- just cleanup.
    pub fn on_finalize(self, f: impl Fn() + Send + Sync + 'static) -> Self {
        let parent = self.factory;
        let f = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullOnFinalize { child, finalizer: Some(f) })
        })
    }

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
}
