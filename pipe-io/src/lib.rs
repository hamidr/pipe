//! I/O constructors for `pipe` -- ergonomic sources and sinks for
//! files, TCP, and UDP.
//!
//! ```ignore
//! use pipe_io::net;
//!
//! net::tcp_server("0.0.0.0:8080".parse()?)
//!     .map(|conn| {
//!         let (lines, writer) = conn.into_lines();
//!         lines.eval_map(move |line| {
//!             let writer = writer.clone();
//!             async move {
//!                 writer.write_all(b"hello\n").await?;
//!                 Ok(line)
//!             }
//!         })
//!     })
//!     .par_join_unbounded()
//!     .for_each(|msg| println!("{msg}"))
//!     .await?;
//! ```

pub mod file;
pub mod net;
