//! Bounded async channel for connecting pipe stages with backpressure.
//!
//! The sender blocks (awaits) when the buffer is full — this IS backpressure.
//! The receiver blocks when empty — this IS demand-driven pull.
//!
//! Used internally by `Pipe::prefetch`, `Pipe::merge`, and
//! `Pipe::broadcast`.

use crate::pull::{ChunkFut, PipeError, PullOperator};

/// Create a bounded channel that transports chunks of `B`.
///
/// `capacity` is the number of chunks (not elements) the buffer can hold.
/// The sender awaits when full; the receiver awaits when empty.
pub fn bounded<B: Send + 'static>(capacity: usize) -> (Sender<B>, Receiver<B>) {
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<B>>(capacity.max(1));
    (Sender { tx }, Receiver { rx, abort: None })
}

/// Sending half of a bounded channel. Cloneable for fan-in patterns.
pub struct Sender<B> {
    tx: tokio::sync::mpsc::Sender<Vec<B>>,
}

impl<B> Clone for Sender<B> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<B: Send + 'static> Sender<B> {
    /// Send a chunk. Awaits if buffer is full (backpressure).
    pub async fn send(&self, chunk: Vec<B>) -> Result<(), PipeError> {
        self.tx
            .send(chunk)
            .await
            .map_err(|_| PipeError::Closed)
    }
}

/// Receiving half. Implements [`PullOperator<B>`] for direct use in pipes.
///
/// If an `AbortHandle` is attached, the background task is cancelled
/// when the receiver is dropped — providing automatic resource cleanup.
pub struct Receiver<B> {
    rx: tokio::sync::mpsc::Receiver<Vec<B>>,
    /// Optional abort handle for the producing background task.
    abort: Option<tokio::task::AbortHandle>,
}

impl<B> Drop for Receiver<B> {
    fn drop(&mut self) {
        if let Some(handle) = self.abort.take() {
            handle.abort();
        }
    }
}

impl<B> Receiver<B> {
    /// Create a receiver from a raw tokio mpsc receiver.
    pub fn from_mpsc(rx: tokio::sync::mpsc::Receiver<Vec<B>>) -> Self {
        Self { rx, abort: None }
    }

    /// Attach an abort handle for automatic cancellation on drop.
    pub fn with_abort(mut self, handle: tokio::task::AbortHandle) -> Self {
        self.abort = Some(handle);
        self
    }
}

impl<B: Send + 'static> PullOperator<B> for Receiver<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move { Ok(self.rx.recv().await) })
    }
}

/// Receives individual items from a background task.
/// Aborts the task on drop.
pub struct TaskReceiver<B> {
    rx: tokio::sync::mpsc::Receiver<B>,
    abort: tokio::task::AbortHandle,
}

impl<B> TaskReceiver<B> {
    /// Create from a raw mpsc receiver and an abort handle.
    pub fn new(rx: tokio::sync::mpsc::Receiver<B>, abort: tokio::task::AbortHandle) -> Self {
        Self { rx, abort }
    }
}

impl<B> Drop for TaskReceiver<B> {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

impl<B: Send + 'static> PullOperator<B> for TaskReceiver<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.rx.recv().await {
                Some(item) => Ok(Some(vec![item])),
                None => Ok(None),
            }
        })
    }
}

/// Receives `Result<B, PipeError>` items from a background task.
/// Aborts the task on drop. Propagates errors to the consumer.
pub struct TaskResultReceiver<B> {
    rx: tokio::sync::mpsc::Receiver<Result<B, PipeError>>,
    abort: tokio::task::AbortHandle,
}

impl<B> TaskResultReceiver<B> {
    /// Create from a raw mpsc receiver and an abort handle.
    pub fn new(
        rx: tokio::sync::mpsc::Receiver<Result<B, PipeError>>,
        abort: tokio::task::AbortHandle,
    ) -> Self {
        Self { rx, abort }
    }
}

impl<B> Drop for TaskResultReceiver<B> {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

impl<B: Send + 'static> PullOperator<B> for TaskResultReceiver<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async move {
            match self.rx.recv().await {
                Some(Ok(item)) => Ok(Some(vec![item])),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_recv_basic() {
        let (tx, mut rx) = bounded::<i64>(2);
        tx.send(vec![1, 2, 3]).await.unwrap();
        tx.send(vec![4, 5]).await.unwrap();
        drop(tx);

        let c1 = rx.next_chunk().await.unwrap().unwrap();
        assert_eq!(c1, vec![1, 2, 3]);
        let c2 = rx.next_chunk().await.unwrap().unwrap();
        assert_eq!(c2, vec![4, 5]);
        assert!(rx.next_chunk().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn backpressure_blocks_sender() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let (tx, mut rx) = bounded::<i64>(1); // capacity 1 chunk
        let sent = Arc::new(AtomicUsize::new(0));
        let sent2 = sent.clone();

        // Spawn sender that tries to send 3 chunks
        let handle = tokio::spawn(async move {
            for i in 0..3 {
                tx.send(vec![i]).await.unwrap();
                sent2.fetch_add(1, Ordering::SeqCst);
            }
        });

        // Give sender time to fill buffer
        tokio::task::yield_now().await;

        // With capacity 1, at most 1 chunk should be buffered
        // (sender may have sent 1-2 depending on timing)
        let first = rx.next_chunk().await.unwrap().unwrap();
        assert_eq!(first.len(), 1);

        // Drain remaining
        let _ = rx.next_chunk().await.unwrap();
        let _ = rx.next_chunk().await.unwrap();

        handle.await.unwrap();
        assert_eq!(sent.load(Ordering::SeqCst), 3);
    }
}
