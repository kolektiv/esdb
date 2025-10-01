#![allow(clippy::multiple_crate_versions)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

mod model;
mod persistence;

pub use self::{
    model::Stream,
    persistence::Store,
};
