//! Generic async pipeline engine — composable, typed, pull-based data flows.
//!
//! `knot-pipe` provides a lazy effectful list (`Pipe<B>`, analogous to
//! FS2's `Stream[F, O]`) and per-element operators. It is the streaming
//! engine underneath Knot's graph query system, but can power any domain:
//! data processing, HTTP middleware, ML inference, event streaming.
//!
//! `B` is the **element type** — chunking is an internal detail.
//!
//! # Example
//!
//! ```ignore
//! use knot_pipe::prelude::*;
//!
//! let result = Pipe::from_iter(1..=10)
//!     .map(|x| x * 2)
//!     .filter(|x| *x > 10)
//!     .take(3)
//!     .collect().await?;
//! assert_eq!(result, vec![12, 14, 16]);
//! ```
//!
//! # Async operators
//!
//! For effectful per-element transforms, implement [`Operator`](crate::operator::Operator)
//! and use `.pipe(op)`. Operators capture their own state:
//!
//! ```ignore
//! #[derive(Debug)]
//! struct FetchScore { client: HttpClient }
//!
//! impl Operator<UserId, UserId> for FetchScore {
//!     fn execute<'a>(&'a self, id: UserId) -> PinFut<'a, UserId> {
//!         Box::pin(async move { /* async I/O via self.client */ Ok(id) })
//!     }
//! }
//! ```

pub mod cancel;
pub mod channel;
pub mod io;
pub mod operator;
pub mod pipeline;
pub mod pull;
pub mod sink;
pub mod stream;
pub mod transform;

pub mod prelude {
    //! Re-exports for convenience.
    pub use crate::cancel::CancelToken;
    pub use crate::channel::{bounded, Receiver, Sender};
    pub use crate::operator::{Operator, PinFut};
    pub use crate::pipeline::{Emitter, Pipe};
    pub use crate::pull::{ChunkFut, PipeError, PullOperator};
    pub use crate::sink::Sink;
    pub use crate::transform::Transform;
}
