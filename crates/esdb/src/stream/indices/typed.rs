mod ordinal;

use derive_more::Debug;
use fjall::{
    Batch,
    PartitionHandle,
};

use crate::stream::{
    Append,
    indices::typed::ordinal::Ordinal,
};

#[derive(Debug)]
pub struct Typed {
    ordinal: Ordinal,
}

impl Typed {
    pub fn new(indices: &PartitionHandle) -> Self {
        let ordinal = Ordinal::new(indices.clone());

        Self { ordinal }
    }
}

impl Append for Typed {
    fn append(&self, batch: &mut Batch, event: &(&[u8], [u8; 8]), index: [u8; 8]) {
        self.ordinal.append(batch, event, index);
    }
}
