mod typed;

use std::error::Error;

use derive_more::Debug;
use fjall::{
    Batch,
    PartitionCreateOptions,
};

use crate::stream::{
    Append,
    data::Data,
    indices::typed::Typed,
};

static INDICES: &str = "indices";

#[derive(Debug)]
pub struct Indices {
    typed: Typed,
}

impl Indices {
    pub fn new(data: &Data) -> Result<Self, Box<dyn Error>> {
        let indices_options = PartitionCreateOptions::default();
        let indices = data.keyspace.open_partition(INDICES, indices_options)?;

        let typed = Typed::new(&indices);

        Ok(Self { typed })
    }
}

impl Append for Indices {
    fn append(&self, batch: &mut Batch, event: &(&[u8], [u8; 8]), index: [u8; 8]) {
        self.typed.append(batch, event, index);
    }
}
