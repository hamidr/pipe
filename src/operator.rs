//! Core operator trait -- per-element, async, type-changing.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

/// Convenience type alias for boxed futures.
pub type PinFut<'a, B> =
    Pin<Box<dyn Future<Output = Result<B, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>>;

/// A per-element operator that transforms `A` into `B` asynchronously.
///
/// Operators that need external state (database, HTTP client, etc.)
/// capture it at construction via `Arc`, closures, etc.
///
/// CPU-bound operators wrap sync logic in `Box::pin(async move { ... })`.
/// I/O operators await inside the future.
pub trait Operator<A: Send + 'static, B: Send + 'static>: Send + fmt::Debug {
    /// Transform one element asynchronously.
    fn execute<'a>(&'a self, input: A) -> PinFut<'a, B>;
}
