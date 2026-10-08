//! An alternative HTML frontend for Gmail, rendered entirely on the server

mod html;

mod model;

mod server;
pub use server::serve;
