#![allow(clippy::multiple_crate_versions)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

mod model;
pub mod persistence;

pub use self::{
    model::{
        Event,
        Stream,
    },
    persistence::Database,
};
