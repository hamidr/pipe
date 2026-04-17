//! Server-side gRPC streaming response.
//!
//! Converts a `Pipe<T>` into a stream that tonic server handlers can
//! return directly. Bridges pipe's `PipeError` to `tonic::Status`.
//!
//! ```ignore
//! use lazyflow_grpc::serve;
//!
//! #[tonic::async_trait]
//! impl MyService for MyServer {
//!     type StreamStream = serve::PipeResponse<MyItem>;
//!
//!     async fn stream(
//!         &self,
//!         request: Request<MyRequest>,
//!     ) -> Result<Response<Self::StreamStream>, Status> {
//!         let pipe = Pipe::from_iter(vec![item1, item2, item3])
//!             .filter(|i| i.is_valid())
//!             .map(|i| transform(i));
//!         Ok(Response::new(serve::to_stream(pipe)))
//!     }
//! }
//! ```

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use lazyflow::pipeline::Pipe;
use lazyflow::pull::PipeError;

/// A tonic-compatible response stream backed by a `Pipe<T>`.
///
/// Implements `Stream<Item = Result<T, tonic::Status>>`, which is
/// the signature tonic expects from server-streaming RPC return types.
///
/// Created by [`to_stream`].
pub struct PipeResponse<T: Send + 'static> {
    inner: lazyflow::stream::PipeStream<T>,
}

impl<T: Send + 'static> Stream for PipeResponse<T> {
    type Item = Result<T, tonic::Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<T, tonic::Status>>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(pipe_error_to_status(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Convert a `Pipe<T>` into a tonic-compatible streaming response.
///
/// The pipe runs in a background task (via `Pipe::into_stream`).
/// Backpressure propagates through the bounded channel: a slow client
/// slows the pipe. Dropping the response stream cancels the pipe.
pub fn to_stream<T: Send + 'static>(pipe: Pipe<T>) -> PipeResponse<T> {
    PipeResponse {
        inner: pipe.into_stream(),
    }
}

/// Convert a `Pipe<T>` into a tonic-compatible streaming response
/// with a custom buffer size for backpressure tuning.
pub fn to_stream_buffered<T: Send + 'static>(pipe: Pipe<T>, buffer_size: usize) -> PipeResponse<T> {
    PipeResponse {
        inner: pipe.into_stream_buffered(buffer_size),
    }
}

/// Map a `PipeError` to `tonic::Status`.
///
/// - `PipeError::Io` -> `Status::internal`
/// - `PipeError::Closed` -> `Status::cancelled`
/// - `PipeError::RetryExhausted` -> `Status::unavailable`
/// - `PipeError::Custom` containing a `tonic::Status` -> unwrapped as-is
/// - `PipeError::Custom` (other) -> `Status::internal`
pub fn pipe_error_to_status(err: PipeError) -> tonic::Status {
    match err {
        PipeError::Io(e) => tonic::Status::internal(e.to_string()),
        PipeError::Closed => tonic::Status::cancelled("pipe closed"),
        PipeError::RetryExhausted => tonic::Status::unavailable("retry exhausted"),
        PipeError::Custom(e) => match e.downcast::<tonic::Status>() {
            Ok(status) => *status,
            Err(other) => tonic::Status::internal(other.to_string()),
        },
    }
}
