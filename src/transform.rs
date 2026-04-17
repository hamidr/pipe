//! First-class stream transforms.
//!
//! A [`Transform<A, B>`] converts a `Pipe<A>` into a `Pipe<B>`. Unlike
//! closures passed to [`through`](crate::pipeline::Pipe::through),
//! transforms can be named, stored, cloned, and composed.
//!
//! ```ignore
//! use lazyflow::{Pipe, Transform};
//!
//! let t = Transform::new(|p: Pipe<i64>| p.filter(|x| x % 2 == 0).map(|x| x * 10));
//! let u = Transform::new(|p: Pipe<i64>| p.take(3));
//!
//! // Compose: t then u
//! let combined = t.and_then(u);
//!
//! let result = Pipe::from_iter(1..=20)
//!     .through(combined)
//!     .collect().await?;
//! assert_eq!(result, vec![20, 40, 60]);
//! ```

use std::sync::Arc;

use crate::pipeline::Pipe;

/// A reusable, composable stream transform from `Pipe<A>` to `Pipe<B>`.
pub struct Transform<A: Send + 'static, B: Send + 'static> {
    f: Arc<dyn Fn(Pipe<A>) -> Pipe<B> + Send + Sync>,
}

impl<A: Send + 'static, B: Send + 'static> Clone for Transform<A, B> {
    fn clone(&self) -> Self {
        Self {
            f: Arc::clone(&self.f),
        }
    }
}

impl<A: Send + 'static, B: Send + 'static> Transform<A, B> {
    /// Create a transform from a function.
    pub fn new(f: impl Fn(Pipe<A>) -> Pipe<B> + Send + Sync + 'static) -> Self {
        Self { f: Arc::new(f) }
    }

    /// Apply this transform to a pipe.
    pub fn apply(&self, pipe: Pipe<A>) -> Pipe<B> {
        (self.f)(pipe)
    }

    /// Compose this transform with another: `self` then `next`.
    pub fn and_then<C: Send + 'static>(self, next: Transform<B, C>) -> Transform<A, C> {
        Transform {
            f: Arc::new(move |pipe| next.apply(self.apply(pipe))),
        }
    }
}

/// Identity transform -- passes the pipe through unchanged.
impl<A: Send + 'static> Transform<A, A> {
    pub fn identity() -> Self {
        Self::new(|p| p)
    }
}

impl<B: Send + 'static> Pipe<B> {
    /// Apply a [`Transform`] to this pipe.
    ///
    /// This is equivalent to `transform.apply(self)` and also works
    /// with [`through`](Pipe::through).
    pub fn apply<C: Send + 'static>(self, transform: &Transform<B, C>) -> Pipe<C> {
        transform.apply(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn transform_basic() {
        let t = Transform::new(|p: Pipe<i64>| p.filter(|x| x % 2 == 0).map(|x| x * 10));
        let result = Pipe::from_iter(1..=5).apply(&t).collect().await.unwrap();
        assert_eq!(result, vec![20, 40]);
    }

    #[tokio::test]
    async fn transform_clone() {
        let t = Transform::new(|p: Pipe<i64>| p.map(|x| x + 1));
        let t2 = t.clone();
        let a = Pipe::from_iter(vec![1, 2, 3])
            .apply(&t)
            .collect()
            .await
            .unwrap();
        let b = Pipe::from_iter(vec![10, 20])
            .apply(&t2)
            .collect()
            .await
            .unwrap();
        assert_eq!(a, vec![2, 3, 4]);
        assert_eq!(b, vec![11, 21]);
    }

    #[tokio::test]
    async fn transform_compose() {
        let double = Transform::new(|p: Pipe<i64>| p.map(|x| x * 2));
        let take3 = Transform::new(|p: Pipe<i64>| p.take(3));
        let combined = double.and_then(take3);

        let result = Pipe::from_iter(1..=10)
            .apply(&combined)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![2, 4, 6]);
    }

    #[tokio::test]
    async fn transform_identity() {
        let id = Transform::<i64, i64>::identity();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .apply(&id)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn transform_type_changing() {
        let to_string = Transform::new(|p: Pipe<i64>| p.map(|x| format!("v{x}")));
        let result = Pipe::from_iter(vec![1, 2, 3])
            .apply(&to_string)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec!["v1", "v2", "v3"]);
    }

    #[tokio::test]
    async fn transform_multi_compose() {
        let evens = Transform::new(|p: Pipe<i64>| p.filter(|x| x % 2 == 0));
        let triple = Transform::new(|p: Pipe<i64>| p.map(|x| x * 3));
        let take2 = Transform::new(|p: Pipe<i64>| p.take(2));

        let pipeline = evens.and_then(triple).and_then(take2);
        let result = Pipe::from_iter(1..=20)
            .apply(&pipeline)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![6, 12]);
    }
}
