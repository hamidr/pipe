//! Reactive state primitive for streaming the latest value.
//!
//! A [`Signal`] holds a current value and emits changes to
//! subscribers as a `Pipe<B>`. Unlike [`Topic`](crate::topic::Topic),
//! a Signal always has a value -- subscribers see the current value
//! immediately, then each subsequent change.
//!
//! ```ignore
//! let signal = Signal::new(0);
//! let sub = signal.subscribe();
//!
//! signal.set(1);
//! signal.set(2);
//! drop(signal); // subscribers complete
//!
//! // sub emits: 0, 1, 2 (may skip intermediate values)
//! ```

use tokio::sync::watch;

use crate::pipeline::Pipe;
use crate::pull::{ChunkFut, PullOperator};

/// A reactive signal that holds a current value.
///
/// Subscribers receive the current value immediately, then each
/// subsequent change. If a subscriber is slow, intermediate values
/// are skipped (only the latest is seen).
///
/// The signal closes when all `Signal` handles are dropped.
/// Subscriber pipes then complete.
#[derive(Clone)]
pub struct Signal<B: Clone + Send + Sync + 'static> {
    tx: watch::Sender<B>,
    rx: watch::Receiver<B>,
}

impl<B: Clone + Send + Sync + 'static> Signal<B> {
    /// Create a signal with an initial value.
    pub fn new(initial: B) -> Self {
        let (tx, rx) = watch::channel(initial);
        Self { tx, rx }
    }

    /// Get the current value.
    pub fn get(&self) -> B {
        self.rx.borrow().clone()
    }

    /// Set a new value, notifying all subscribers.
    pub fn set(&self, value: B) {
        let _ = self.tx.send(value);
    }

    /// Modify the current value in place, notifying subscribers.
    pub fn modify(&self, f: impl FnOnce(&mut B)) {
        self.tx.send_modify(f);
    }

    /// Subscribe to value changes as a `Pipe<B>`.
    ///
    /// The pipe emits the current value immediately, then each
    /// subsequent change. Intermediate values may be skipped if
    /// the subscriber is slow. The pipe completes when all `Signal`
    /// handles are dropped.
    pub fn subscribe(&self) -> Pipe<B> {
        let rx = self.rx.clone();
        Pipe::from_pull_once(SignalReceiver {
            rx,
            emitted_initial: false,
        })
    }
}

struct SignalReceiver<B: Clone + Send + Sync + 'static> {
    rx: watch::Receiver<B>,
    emitted_initial: bool,
}

impl<B: Clone + Send + Sync + 'static> PullOperator<B> for SignalReceiver<B> {
    fn next_chunk(&mut self) -> ChunkFut<'_, B> {
        Box::pin(async {
            if !self.emitted_initial {
                self.emitted_initial = true;
                let val = self.rx.borrow_and_update().clone();
                return Ok(Some(vec![val]));
            }
            match self.rx.changed().await {
                Ok(()) => {
                    let val = self.rx.borrow_and_update().clone();
                    Ok(Some(vec![val]))
                }
                Err(_) => Ok(None),
            }
        })
    }
}

impl<B: Clone + Send + Sync + 'static> Pipe<B> {
    /// Convert a pipe into a signal that tracks the latest value.
    ///
    /// Returns a `Signal` initialized with `initial`. As elements
    /// flow through the pipe, the signal is updated. Spawns a
    /// background task to drain the pipe.
    pub fn hold(self, initial: B) -> Signal<B> {
        let signal = Signal::new(initial);
        let s = signal.clone();
        tokio::spawn(async move {
            let _ = self.for_each(|x| s.set(x)).await;
        });
        signal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signal_emits_initial_then_closes() {
        let signal = Signal::new(42);
        let sub = signal.subscribe();
        drop(signal);

        let result = sub.collect().await.unwrap();
        // Subscriber sees the initial value, then completes
        assert_eq!(result, vec![42]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signal_sees_updates_before_drop() {
        let signal = Signal::new(0);
        signal.set(1);
        signal.set(2);

        // Subscribe after updates -- sees latest value (2)
        let sub = signal.subscribe();
        drop(signal);

        let result = sub.collect().await.unwrap();
        assert_eq!(result, vec![2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signal_get_returns_current() {
        let signal = Signal::new(42);
        assert_eq!(signal.get(), 42);
        signal.set(99);
        assert_eq!(signal.get(), 99);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signal_modify() {
        let signal = Signal::new(vec![1, 2]);
        signal.modify(|v| v.push(3));
        assert_eq!(signal.get(), vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signal_multiple_subscribers() {
        let signal = Signal::new(0);
        let sub1 = signal.subscribe();
        let sub2 = signal.subscribe();

        let h1 = tokio::spawn(async move { sub1.collect().await.unwrap() });
        let h2 = tokio::spawn(async move { sub2.collect().await.unwrap() });

        tokio::task::yield_now().await;
        signal.set(1);
        tokio::task::yield_now().await;
        drop(signal);

        let r1 = h1.await.unwrap();
        let r2 = h2.await.unwrap();
        // Both see the final value (intermediate may differ due to timing)
        assert_eq!(*r1.last().unwrap(), 1);
        assert_eq!(*r2.last().unwrap(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hold_converts_pipe_to_signal() {
        let signal = Pipe::from_iter(vec![1, 2, 3]).hold(0);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(signal.get(), 3);
    }
}
