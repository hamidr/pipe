//! Server-streaming gRPC source.
//!
//! Wraps tonic's `Streaming<T>` into a `Pipe<T>`. The pipe is
//! single-use (same as `Pipe::generate_once`) because `Streaming<T>`
//! is an owned, non-cloneable resource.
//!
//! No automatic reconnection -- gRPC has no built-in resume protocol
//! (unlike SSE's Last-Event-ID). Use pipe's `retry()` or
//! `handle_error_with()` for application-level reconnection.

use lazyflow::pipeline::Pipe;
use lazyflow::pull::PipeError;

/// Convert a tonic `Streaming<T>` into a `Pipe<T>`.
///
/// The returned pipe is single-use -- cloning and materializing both
/// clones returns an error on the second materialization.
///
/// Cancellation: dropping the pipe drops the `Streaming<T>`, which
/// resets the HTTP/2 stream.
///
/// ```ignore
/// let response = client.server_stream(Request::new(req)).await?;
/// let pipe = lazyflow_grpc::streaming::from_tonic(response.into_inner());
/// let items = pipe.take(10).collect().await?;
/// ```
pub fn from_tonic<T>(stream: tonic::Streaming<T>) -> Pipe<T>
where
    T: Send + 'static,
{
    Pipe::generate_once(move |tx| async move {
        let mut stream = stream;
        loop {
            match stream.message().await {
                Ok(Some(msg)) => tx.emit(msg).await?,
                Ok(None) => return Ok(()),
                Err(status) => return Err(status_to_pipe_error(status)),
            }
        }
    })
}

/// Map a tonic `Status` to `PipeError`.
///
/// Exposed for use in error-handling pipelines where users need to
/// convert tonic errors outside the streaming wrapper.
pub fn status_to_pipe_error(status: tonic::Status) -> PipeError {
    PipeError::Custom(Box::new(status))
}
