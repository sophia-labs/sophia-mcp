//! sophia-mcp — library surface.
//!
//! The `sophia-mcp` binary is a thin wrapper over this crate. Exposed so integration
//! tests (and any future embedders) can drive the proxy core directly.

pub mod backend;
pub mod config;
pub mod mcp;
pub mod server;
