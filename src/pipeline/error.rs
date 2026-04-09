//! Error recovery operators: handle_error_with, retry, bracket.

use std::sync::Arc;

use crate::pull::PipeError;

use super::pull_ops::{BracketState, PullBracket, PullHandleError, PullRetry};
use super::Pipe;

impl<B: Send + 'static> Pipe<B> {
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
}
