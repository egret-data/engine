//! PostgreSQL wire protocol frontend for Egret Engine.

mod error;
mod handler;
mod server;

pub use error::{INTERNAL_ERROR, SYNTAX_ERROR, sqlstate, to_pg_error};
pub use server::{PgWireServer, SessionFactory};
