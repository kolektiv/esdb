mod types;

use std::error::Error;

use derive_more::Debug;
use fjall::{
    Batch,
    PartitionCreateOptions,
};

use crate::{
    data::Data,
    stream::indices::types::Types,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct HashedTypeSpecifier(u64, u8);

impl From<(u64, u8)> for HashedTypeSpecifier {
    fn from(value: (u64, u8)) -> Self {
        Self(value.0, value.1)
    }
}

static INDICES: &str = "indices";

#[derive(Debug)]
pub struct Indices {
    types: Types,
}

impl Indices {
    pub fn new(data: &Data) -> Result<Self, Box<dyn Error>> {
        let indices_options = PartitionCreateOptions::default();
        let indices = data.keyspace.open_partition(INDICES, indices_options)?;

        let types = Types::new(&indices);

        Ok(Self { types })
    }
}

impl Indices {
    pub fn insert(&self, batch: &mut Batch, type_specifier: &HashedTypeSpecifier, position: u64) {
        self.types.insert(batch, type_specifier, position);
    }
}
