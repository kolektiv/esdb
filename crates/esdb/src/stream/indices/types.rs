pub mod forward;

use derive_more::Debug;
use fjall::{
    Batch,
    PartitionHandle,
};

use crate::stream::indices::{
    HashedTypeSpecifier,
    types::forward::Forward,
};

#[derive(Debug)]
pub struct Types {
    forward: Forward,
}

impl Types {
    pub fn new(indices: &PartitionHandle) -> Self {
        let forward = Forward::new(indices.clone());

        Self { forward }
    }
}

impl Types {
    pub fn insert(&self, batch: &mut Batch, type_specifier: &HashedTypeSpecifier, position: u64) {
        self.forward.insert(batch, type_specifier, position);
    }
}
