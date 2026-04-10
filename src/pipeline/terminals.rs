//! Terminal operations and specialized impls: collect, fold, count, for_each,
//! first, last, into_pull, unchunks, flatten, unzip.

use std::collections::VecDeque;

use crate::pull::{collect_all, fold_all, PipeError, PullOperator};

use super::pull_ops::PullFlatten;
use super::Pipe;

impl<B: Send + 'static> Pipe<B> {
    /// Materialize the pipeline into a pull operator chain.
    pub(crate) fn into_pull(self) -> Box<dyn PullOperator<B>> {
        (self.factory)()
    }

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

/// Flatten a `Pipe<Pipe<B>>` into `Pipe<B>`.
impl<B: Send + 'static> Pipe<Pipe<B>> {
    /// Flatten nested pipes: each inner pipe is drained sequentially.
    pub fn flatten(self) -> Pipe<B> {
        let parent = self.factory;
        Pipe::from_factory(move || {
            Box::new(PullFlatten {
                outer: parent(),
                inner: None,
                pending: VecDeque::new(),
            })
        })
    }
}

/// Split a `Pipe<(A, B)>` into two pipes via broadcast.
///
/// Uses `broadcast(2)` internally — the source is shared.
/// Both pipes must be consumed; dropping one stalls the other.
impl<A: Clone + Send + Sync + 'static, B: Clone + Send + Sync + 'static> Pipe<(A, B)> {
    pub fn unzip(self, buffer_size: usize) -> (Pipe<A>, Pipe<B>) {
        let branches = self.broadcast(2, buffer_size);
        let mut iter = branches.into_iter();
        let left = iter.next().unwrap().map(|(a, _)| a);
        let right = iter.next().unwrap().map(|(_, b)| b);
        (left, right)
    }
}
