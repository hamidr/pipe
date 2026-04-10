//! Time-based operators: timeout, throttle, debounce, chunks_timeout.

use super::pull_ops::{PullThrottle, PullTimeout};
use super::Pipe;

impl<B: Send + 'static> Pipe<B> {
    /// Fail if a single pull takes longer than `duration`.
    pub fn timeout(self, duration: std::time::Duration) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            Box::new(PullTimeout {
                child: parent(),
                duration,
            })
        })
    }

    /// Emit at most one element per `duration`.
    ///
    /// Elements arriving faster than the rate are delayed, not dropped.
    pub fn throttle(self, duration: std::time::Duration) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            Box::new(PullThrottle {
                child: parent(),
                duration,
                last: None,
            })
        })
    }

    /// Emit an element only after `duration` of silence.
    ///
    /// If new elements keep arriving within `duration`, only the
    /// latest is emitted once the quiet period elapses. Spawns a
    /// background task at materialization.
    pub fn debounce(self, duration: std::time::Duration) -> Self {
        let parent = self.factory;
        Self::from_factory(move || {
            let (tx, rx) = tokio::sync::mpsc::channel::<B>(1);
            let mut root = parent();

            let handle = tokio::spawn(async move {
                let mut latest: Option<B> = None;

                loop {
                    if latest.is_some() {
                        // We have a pending item -- wait for quiet period or new item
                        tokio::select! {
                            biased;
                            result = root.next_chunk() => {
                                match result {
                                    Ok(Some(chunk)) => {
                                        // New data -- reset timer, keep latest
                                        latest = chunk.into_iter().last();
                                    }
                                    Ok(None) | Err(_) => {
                                        // Source done -- flush latest and exit
                                        if let Some(item) = latest.take() {
                                            let _ = tx.send(item).await;
                                        }
                                        return;
                                    }
                                }
                            }
                            _ = tokio::time::sleep(duration) => {
                                // Quiet period elapsed -- emit
                                if let Some(item) = latest.take() {
                                    if tx.send(item).await.is_err() { return; }
                                }
                            }
                        }
                    } else {
                        // No pending item -- wait for next chunk
                        match root.next_chunk().await {
                            Ok(Some(chunk)) => {
                                latest = chunk.into_iter().last();
                            }
                            Ok(None) | Err(_) => return,
                        }
                    }
                }
            });

            Box::new(crate::channel::TaskReceiver::new(rx, handle.abort_handle()))
        })
    }

    /// Batch elements by count OR time, whichever triggers first.
    ///
    /// Collects up to `max_size` elements into a `Vec<B>`. If the
    /// deadline elapses before the batch is full, flushes whatever
    /// has accumulated. Spawns a background task at materialization.
    pub fn chunks_timeout(
        self,
        max_size: usize,
        timeout: std::time::Duration,
    ) -> Pipe<Vec<B>> {
        let parent = self.factory;
        Pipe::from_factory(move || {
            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<B>>(2);
            let mut root = parent();
            let handle = tokio::spawn(async move {
                let mut batch = Vec::with_capacity(max_size);
                loop {
                    let deadline = tokio::time::sleep(timeout);
                    tokio::pin!(deadline);

                    // Pull chunks until batch is full or deadline fires
                    loop {
                        // Flush any complete batches already buffered
                        while batch.len() >= max_size {
                            let ready = batch.drain(..max_size).collect();
                            if tx.send(ready).await.is_err() { return; }
                        }

                        tokio::select! {
                            biased;
                            result = root.next_chunk() => {
                                match result {
                                    Ok(Some(chunk)) => {
                                        batch.extend(chunk);
                                        // Loop back to flush any full batches
                                    }
                                    Ok(None) => {
                                        // Source exhausted -- flush remainder
                                        if !batch.is_empty() {
                                            let _ = tx.send(std::mem::take(&mut batch)).await;
                                        }
                                        return;
                                    }
                                    Err(_) => return,
                                }
                            }
                            _ = &mut deadline => {
                                // Timer fired -- flush partial batch
                                if !batch.is_empty() {
                                    if tx.send(std::mem::take(&mut batch)).await.is_err() { return; }
                                    batch = Vec::with_capacity(max_size);
                                }
                                break; // Reset timer
                            }
                        }
                    }
                }
            });

            Box::new(crate::channel::TaskReceiver::new(rx, handle.abort_handle()))
        })
    }
}
