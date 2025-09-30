use bytes::BufMut;
use derive_more::Debug;
use fjall::{
    Batch,
    PartitionHandle,
};

use crate::stream::indices::HashedTypeSpecifier;

#[derive(Debug)]
pub struct Forward {
    #[debug("PartitionHandle(\"{}\")", indices.name)]
    indices: PartitionHandle,
}

impl Forward {
    pub fn new(indices: PartitionHandle) -> Self {
        Self { indices }
    }
}

impl Forward {
    pub fn insert(&self, batch: &mut Batch, type_specifier: &HashedTypeSpecifier, position: u64) {
        let mut key = [0u8; 17];
        let mut value = [0u8; 1];

        {
            let mut key = &mut key[..];

            key.put_u8(0);
            key.put_u64(type_specifier.0);
            key.put_u64(position);

            let mut value = &mut value[..];

            value.put_u8(type_specifier.1);
        }

        batch.insert(&self.indices, key, value);
    }
}
