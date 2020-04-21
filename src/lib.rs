//! This crate provides wrapper to make [Oxigraph] comply with the [Sophia] API.
//!
//! [Oxigraph]: https://github.com/Tpt/oxigraph
//! [Sophia]: https://docs.rs/sophia/latest/sophia/
#![deny(missing_docs)]

pub mod connection;
pub mod once_toggle;
pub mod quad;
pub mod repository;
pub mod term;
