//! Composable output destinations for pipes.
//!
//! A [`Sink<B>`] consumes a `Pipe<B>` and produces a result. Like
//! [`Transform`](crate::transform::Transform), sinks can be named,
//! stored, and cloned.
//!
//! ```ignore
//! use pipe::{Pipe, Sink};
//!
//! let to_vec = Sink::collect::<Vec<i64>>();
//! let count = Sink::count();
//!
//! let result = Pipe::from_iter(1..=5).drain(to_vec).await?;
//! assert_eq!(result, vec![1, 2, 3, 4, 5]);
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::pipeline::Pipe;
use crate::pull::PipeError;

type SinkFut<R> = Pin<Box<dyn Future<Output = Result<R, PipeError>> + Send>>;

/// A reusable, composable output destination.
///
/// Wraps an async function that consumes a `Pipe<B>` and produces `R`.
pub struct Sink<B: Send + 'static, R: Send + 'static> {
    f: Arc<dyn Fn(Pipe<B>) -> SinkFut<R> + Send + Sync>,
}

impl<B: Send + 'static, R: Send + 'static> Clone for Sink<B, R> {
    fn clone(&self) -> Self {
        Self {
            f: Arc::clone(&self.f),
        }
    }
}

impl<B: Send + 'static, R: Send + 'static> Sink<B, R> {
    /// Create a sink from an async function.
    pub fn new<Fut>(f: impl Fn(Pipe<B>) -> Fut + Send + Sync + 'static) -> Self
    where
        Fut: Future<Output = Result<R, PipeError>> + Send + 'static,
    {
        Self {
            f: Arc::new(move |pipe| Box::pin(f(pipe))),
        }
    }

    /// Consume a pipe with this sink.
    pub async fn run(&self, pipe: Pipe<B>) -> Result<R, PipeError> {
        (self.f)(pipe).await
    }
}

impl<B: Send + 'static> Sink<B, Vec<B>> {
    /// Sink that collects all elements into a Vec.
    pub fn collect() -> Self {
        Self::new(|pipe| async { pipe.collect().await })
    }
}

impl<B: Send + 'static> Sink<B, usize> {
    /// Sink that counts elements.
    pub fn count() -> Self {
        Self::new(|pipe| async { pipe.count().await })
    }
}

impl<B: Send + 'static> Sink<B, Option<B>> {
    /// Sink that returns the first element.
    pub fn first() -> Self {
        Self::new(|pipe| async { pipe.first().await })
    }

    /// Sink that returns the last element.
    pub fn last() -> Self {
        Self::new(|pipe| async { pipe.last().await })
    }
}

impl<B: Send + 'static> Sink<B, ()> {
    /// Sink that discards all elements (drains the pipe).
    pub fn drain() -> Self {
        Self::new(|pipe| async {
            pipe.for_each(|_| {}).await
        })
    }
}

impl<B: Send + 'static> Pipe<B> {
    /// Consume this pipe with a [`Sink`].
    pub async fn drain_to<R: Send + 'static>(
        self,
        sink: &Sink<B, R>,
    ) -> Result<R, PipeError> {
        sink.run(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sink_collect() {
        let sink = Sink::collect();
        let result = Pipe::from_iter(vec![1, 2, 3]).drain_to(&sink).await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn sink_count() {
        let sink = Sink::count();
        let result = Pipe::from_iter(1..=10).drain_to(&sink).await.unwrap();
        assert_eq!(result, 10);
    }

    #[tokio::test]
    async fn sink_first() {
        let sink = Sink::first();
        let result = Pipe::from_iter(vec![10, 20, 30])
            .drain_to(&sink)
            .await
            .unwrap();
        assert_eq!(result, Some(10));
    }

    #[tokio::test]
    async fn sink_drain() {
        use std::sync::{Arc, Mutex};
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let sink = Sink::new(move |pipe: Pipe<i64>| {
            let seen = seen2.clone();
            async move {
                pipe.for_each(move |x| seen.lock().unwrap().push(x)).await
            }
        });
        Pipe::from_iter(vec![1, 2, 3]).drain_to(&sink).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn sink_clone_reuse() {
        let sink: Sink<i64, Vec<i64>> = Sink::collect();
        let s2 = sink.clone();
        let a = Pipe::from_iter(vec![1, 2]).drain_to(&sink).await.unwrap();
        let b = Pipe::from_iter(vec![3, 4, 5]).drain_to(&s2).await.unwrap();
        assert_eq!(a, vec![1, 2]);
        assert_eq!(b, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn sink_writer() {
        let _output = Vec::<u8>::new();
        // writer() needs a 'static writer, so use a shared buffer
        let buf = Arc::new(tokio::sync::Mutex::new(Vec::<u8>::new()));
        let buf2 = Arc::clone(&buf);

        let sink = Sink::new(move |pipe: Pipe<Vec<u8>>| {
            let buf = Arc::clone(&buf2);
            async move {
                let mut w = buf.lock().await;
                pipe.into_writer(&mut *w).await
            }
        });

        Pipe::from_iter(vec![vec![1u8, 2], vec![3]])
            .drain_to(&sink)
            .await
            .unwrap();

        let result = buf.lock().await;
        assert_eq!(*result, vec![1, 2, 3]);
    }
}
