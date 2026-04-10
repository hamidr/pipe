//! Async element operations (eval_map, eval_filter, eval_tap, par_eval_map).

use std::future::Future;
use std::sync::Arc;

use crate::pull::{PipeError, PullOperator};

use super::pull_ops::{PullEvalFilter, PullEvalMap, PullEvalTap};
use super::Pipe;

impl<B: Send + 'static> Pipe<B> {
    /// Async per-element transform (like `map` but with `Future`).
    pub fn eval_map<C: Send + 'static, Fut: Future<Output = Result<C, PipeError>> + Send + 'static>(
        self,
        f: impl Fn(B) -> Fut + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        Pipe::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullEvalMap {
                child,
                f: move |b| f(b),
            })
        })
    }

    /// Async per-element filter.
    pub fn eval_filter<Fut: Future<Output = Result<bool, PipeError>> + Send + 'static>(
        self,
        f: impl Fn(&B) -> Fut + Send + Sync + 'static,
    ) -> Self {
        let parent = self.factory;
        let f = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullEvalFilter {
                child,
                f: move |b: &B| f(b),
            })
        })
    }

    /// Async side-effect on each element, pass through unchanged.
    ///
    /// Requires `B: Sync` because elements are borrowed across await points.
    pub fn eval_tap<Fut: Future<Output = Result<(), PipeError>> + Send + 'static>(
        self,
        f: impl Fn(&B) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        B: Sync,
    {
        let parent = self.factory;
        let f = Arc::new(f);
        Self::from_factory(move || {
            let child = parent();
            let f = Arc::clone(&f);
            Box::new(PullEvalTap {
                child,
                f: move |b: &B| f(b),
            })
        })
    }

    /// Bounded-parallel async transform with ordered output.
    ///
    /// Processes up to `concurrency` elements concurrently. Results are
    /// emitted in the same order as the input. Uses a background task
    /// pool connected by channels.
    pub fn par_eval_map<C: Send + 'static, Fut: Future<Output = Result<C, PipeError>> + Send + 'static>(
        self,
        concurrency: usize,
        f: impl Fn(B) -> Fut + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        let concurrency = concurrency.max(1);
        Pipe::from_factory(move || {
            let (out_tx, out_rx) = tokio::sync::mpsc::channel::<Result<C, PipeError>>(concurrency);
            let mut root = parent();
            let f = Arc::clone(&f);

            let handle = tokio::spawn(async move {
                let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
                let mut pending: std::collections::VecDeque<
                    tokio::sync::oneshot::Receiver<Result<C, PipeError>>,
                > = std::collections::VecDeque::new();

                let mut source_done = false;

                loop {
                    // Fill up to concurrency pending tasks
                    while !source_done && pending.len() < concurrency {
                        match root.next_chunk().await {
                            Ok(Some(chunk)) => {
                                for item in chunk {
                                    let (tx, rx) = tokio::sync::oneshot::channel();
                                    let f = Arc::clone(&f);
                                    let sem = Arc::clone(&semaphore);
                                    tokio::spawn(async move {
                                        let _permit = sem.acquire().await;
                                        let result = f(item).await;
                                        let _ = tx.send(result);
                                    });
                                    pending.push_back(rx);
                                }
                            }
                            Ok(None) => {
                                source_done = true;
                            }
                            Err(e) => {
                                let _ = out_tx.send(Err(e)).await;
                                return;
                            }
                        }
                    }

                    if pending.is_empty() {
                        return; // All done
                    }

                    // Drain completed results in order
                    while let Some(front) = pending.front_mut() {
                        match front.await {
                            Ok(result) => {
                                pending.pop_front();
                                if out_tx.send(result).await.is_err() {
                                    return;
                                }
                            }
                            Err(_) => {
                                // Worker dropped -- shouldn't happen
                                pending.pop_front();
                                let _ = out_tx
                                    .send(Err(PipeError::Custom("worker dropped".into())))
                                    .await;
                                return;
                            }
                        }
                    }
                }
            });

            Box::new(crate::channel::TaskResultReceiver::new(out_rx, handle.abort_handle()))
        })
    }

    /// Bounded-parallel async transform with unordered output.
    ///
    /// Like [`par_eval_map`](Self::par_eval_map) but results are
    /// emitted as soon as they complete, not in input order.
    /// Higher throughput when processing times vary.
    pub fn par_eval_map_unordered<
        C: Send + 'static,
        Fut: Future<Output = Result<C, PipeError>> + Send + 'static,
    >(
        self,
        concurrency: usize,
        f: impl Fn(B) -> Fut + Send + Sync + 'static,
    ) -> Pipe<C> {
        let parent = self.factory;
        let f = Arc::new(f);
        let concurrency = concurrency.max(1);
        Pipe::from_factory(move || {
            let (out_tx, out_rx) = tokio::sync::mpsc::channel::<Result<C, PipeError>>(concurrency);
            let mut root = parent();
            let f = Arc::clone(&f);

            let handle = tokio::spawn(async move {
                let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

                loop {
                    match root.next_chunk().await {
                        Ok(Some(chunk)) => {
                            for item in chunk {
                                let f = Arc::clone(&f);
                                let sem = Arc::clone(&semaphore);
                                let tx = out_tx.clone();
                                // Acquire permit before spawning to enforce concurrency limit
                                let permit = match sem.acquire_owned().await {
                                    Ok(p) => p,
                                    Err(_) => return,
                                };
                                tokio::spawn(async move {
                                    let result = f(item).await;
                                    let _ = tx.send(result).await;
                                    drop(permit);
                                });
                            }
                        }
                        Ok(None) => return,
                        Err(e) => {
                            let _ = out_tx.send(Err(e)).await;
                            return;
                        }
                    }
                }
            });

            Box::new(crate::channel::TaskResultReceiver::new(out_rx, handle.abort_handle()))
        })
    }
}
