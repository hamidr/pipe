//! gRPC connectors for `lazyflow`.
//!
//! - [`streaming`]: Wrap tonic `Streaming<T>` as a `Pipe<T>` source
//!   (client consuming a server-streaming RPC).
//! - [`serve`]: Convert a `Pipe<T>` into a tonic-compatible response
//!   stream (server returning a streaming RPC).
//!
//! ```ignore
//! // Client side: consume a server-streaming RPC
//! use lazyflow_grpc::streaming;
//!
//! let response = client.server_stream(Request::new(req)).await?;
//! let events: Pipe<MyResponse> = streaming::from_tonic(response.into_inner());
//! events
//!     .filter(|e| e.is_active())
//!     .for_each(|e| println!("{e:?}"))
//!     .await?;
//!
//! // Server side: return a pipe as a streaming response
//! use lazyflow_grpc::serve;
//!
//! async fn handle(req: Request<MyRequest>) -> Result<Response<serve::PipeResponse<MyItem>>, Status> {
//!     let pipe = build_pipeline(req.into_inner());
//!     Ok(Response::new(serve::to_stream(pipe)))
//! }
//! ```

pub mod serve;
pub mod streaming;

pub use tonic::transport::Channel;
pub use tonic::{Code, Request, Response, Status, Streaming};

#[cfg(feature = "server")]
pub use tonic::async_trait;
