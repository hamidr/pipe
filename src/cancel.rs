//! Cooperative cancellation for pipes.
//!
//! A [`CancelToken`] signals a pipe to stop producing elements
//! gracefully -- in-flight data drains instead of being aborted.
//!
//! ```ignore
//! let token = CancelToken::new();
//! let pipe = Pipe::from_iter(1..=1_000_000)
//!     .with_cancel(token.clone())
//!     .prefetch(4)
//!     .map(|x| x * 2);
//!
//! // Later: signal shutdown -- source stops, pipeline drains
//! token.cancel();
//! ```

use crate::pull::{ChunkFut, PullOperator};

/// A cooperative cancellation signal.
///
/// Clone the token and share it between the pipe and the controller.
/// Calling [`cancel`](CancelToken::cancel) causes any pipe using
/// [`with_cancel`](crate::pipeline::Pipe::with_cancel) to end
/// gracefully on its next pull.
#[derive(Clone)]
pub struct CancelToken {
    tx: std::sync::Arc<tokio::sync::watch::Sender<bool>>,
    rx: tokio::sync::watch::Receiver<bool>,
}

impl CancelToken {
    /// Create a new cancellation token.
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);
        Self {
            tx: std::sync::Arc::new(tx),
            rx,
        }
    }

    /// Signal cancellation. All pipes using this token will stop
    /// producing elements on their next pull.
    pub fn cancel(&self) {
        let _ = self.tx.send(true);
    }

    /// Check if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct PullCancel<B: Send + 'static> {
    pub(crate) child: Box<dyn PullOperator<B>>,
    pub(crate) token: CancelToken,
}

impl<B: Send + 'static> PullOperator<B> for PullCancel<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            if self.token.is_cancelled() {
                return Ok(None);
            }
            self.child.next_chunk().await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::Pipe;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_stops_pipe() {
        let token = CancelToken::new();
        let t = token.clone();

        let pipe = Pipe::from_iter(1..=1000)
            .with_cancel(token)
            .map(move |x| {
                if x == 5 {
                    t.cancel();
                }
                x
            });

        let result = pipe.collect().await.unwrap();
        // Should stop shortly after element 5
        assert!(result.len() <= 261); // At most one full chunk (256) + partial
        assert!(result.contains(&5));
    }

    #[tokio::test]
    async fn cancel_before_start() {
        let token = CancelToken::new();
        token.cancel();

        let result = Pipe::from_iter(vec![1, 2, 3])
            .with_cancel(token)
            .collect()
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn no_cancel_passes_through() {
        let token = CancelToken::new();
        let result = Pipe::from_iter(vec![1, 2, 3])
            .with_cancel(token)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }
}
