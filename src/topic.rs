//! Pub/sub broadcast primitive for multi-subscriber streaming.
//!
//! A [`Topic`] lets multiple subscribers receive the same published
//! messages. Each subscriber gets its own bounded queue -- when the
//! buffer fills, the slowest subscriber skips to catch up.
//!
//! ```ignore
//! let topic = Topic::new(16);
//!
//! // Subscribe before publishing to avoid missing messages
//! let sub1 = topic.subscribe();
//! let sub2 = topic.subscribe();
//!
//! // Publish from a task
//! tokio::spawn({
//!     let topic = topic.clone();
//!     async move {
//!         for i in 0..5 { topic.publish(i).unwrap(); }
//!         topic.close();
//!     }
//! });
//!
//! // Each subscriber is a Pipe<B>
//! let messages = sub1.collect().await?;
//! ```

use tokio::sync::{broadcast, watch};

use crate::pipeline::Pipe;
use crate::pull::{ChunkFut, PipeError, PullOperator};

/// A multi-subscriber broadcast topic.
///
/// Publishers send messages via [`publish`](Self::publish). Each
/// subscriber created via [`subscribe`](Self::subscribe) receives
/// all messages published after subscription. Messages published
/// before a subscription are not seen.
///
/// Call [`close`](Self::close) to signal completion -- all
/// subscriber pipes will drain and terminate.
#[derive(Clone, Debug)]
pub struct Topic<B: Clone + Send + 'static> {
    tx: broadcast::Sender<B>,
    close_tx: watch::Sender<bool>,
    close_rx: watch::Receiver<bool>,
}

impl<B: Clone + Send + Sync + 'static> Topic<B> {
    /// Create a topic with the given buffer capacity.
    ///
    /// `capacity` is the number of messages buffered per subscriber.
    /// If a subscriber falls behind by more than `capacity` messages,
    /// it skips to the latest.
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity.max(1));
        let (close_tx, close_rx) = watch::channel(false);
        Self {
            tx,
            close_tx,
            close_rx,
        }
    }

    /// Publish a single message to all subscribers.
    ///
    /// Returns the number of subscribers that received the message.
    /// Returns 0 if there are no active subscribers.
    pub fn publish(&self, value: B) -> Result<usize, PipeError> {
        if *self.close_rx.borrow() {
            return Err(PipeError::Closed);
        }
        match self.tx.send(value) {
            Ok(n) => Ok(n),
            Err(_) => Ok(0),
        }
    }

    /// Create a subscriber pipe that receives all future messages.
    ///
    /// Subscribe before publishing to avoid missing messages.
    /// The pipe completes when [`close`](Self::close) is called.
    pub fn subscribe(&self) -> Pipe<B> {
        let rx = self.tx.subscribe();
        let close_rx = self.close_rx.clone();
        Pipe::from_pull_once(TopicReceiver { rx, close_rx })
    }

    /// Close the topic. All subscriber pipes will complete after
    /// draining buffered messages. Further publishes return `Err`.
    pub fn close(&self) {
        let _ = self.close_tx.send(true);
    }
}

struct TopicReceiver<B: Clone + Send + 'static> {
    rx: broadcast::Receiver<B>,
    close_rx: watch::Receiver<bool>,
}

impl<B: Clone + Send + 'static> PullOperator<B> for TopicReceiver<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async {
            loop {
                // Check if already closed, drain remaining
                if *self.close_rx.borrow() {
                    return self.drain_remaining();
                }
                tokio::select! {
                    result = self.rx.recv() => {
                        match result {
                            Ok(val) => return Ok(Some(vec![val])),
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => return Ok(None),
                        }
                    }
                    _ = self.close_rx.changed() => {
                        return self.drain_remaining();
                    }
                }
            }
        })
    }
}

impl<B: Clone + Send + 'static> TopicReceiver<B> {
    fn drain_remaining(&mut self) -> Result<Option<Vec<B>>, PipeError> {
        loop {
            match self.rx.try_recv() {
                Ok(val) => return Ok(Some(vec![val])),
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(_) => return Ok(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_subscriber() {
        let topic = Topic::new(16);
        let sub = topic.subscribe();

        let t = topic.clone();
        tokio::spawn(async move {
            for i in 0..5 {
                t.publish(i).unwrap();
            }
            t.close();
        });

        let result = sub.collect().await.unwrap();
        assert_eq!(result, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multiple_subscribers() {
        let topic = Topic::new(16);
        let sub1 = topic.subscribe();
        let sub2 = topic.subscribe();

        let t = topic.clone();
        tokio::spawn(async move {
            for i in 0..3 {
                t.publish(i).unwrap();
            }
            t.close();
        });

        let r1 = sub1.collect().await.unwrap();
        let r2 = sub2.collect().await.unwrap();
        assert_eq!(r1, vec![0, 1, 2]);
        assert_eq!(r2, vec![0, 1, 2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn late_subscriber_misses_early_messages() {
        let topic = Topic::new(16);
        // Keep a subscriber alive so publish succeeds
        let _anchor = topic.subscribe();

        topic.publish(1).unwrap();
        topic.publish(2).unwrap();

        let sub = topic.subscribe();

        topic.publish(3).unwrap();
        drop(_anchor);
        topic.close();

        let result = sub.collect().await.unwrap();
        assert_eq!(result, vec![3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn with_pipe_operators() {
        let topic = Topic::new(16);
        let sub = topic.subscribe();

        let t = topic.clone();
        tokio::spawn(async move {
            for i in 1..=6 {
                t.publish(i).unwrap();
            }
            t.close();
        });

        let result = sub
            .filter(|x| x % 2 == 0)
            .map(|x| x * 10)
            .collect()
            .await
            .unwrap();
        assert_eq!(result, vec![20, 40, 60]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscriber_completes_on_close() {
        let topic = Topic::new(16);
        let sub = topic.subscribe();

        topic.publish(1).unwrap();
        topic.close();

        let result = sub.collect().await.unwrap();
        assert_eq!(result, vec![1]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_with_no_subscribers() {
        let topic = Topic::<i32>::new(16);
        assert_eq!(topic.publish(1).unwrap(), 0);
    }
}
