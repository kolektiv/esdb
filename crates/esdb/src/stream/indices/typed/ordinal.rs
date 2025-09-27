use bytes::{
    BufMut,
    BytesMut,
};
use derive_more::Debug;
use fjall::{
    Batch,
    PartitionHandle,
};

use crate::stream::Append;

static TYPED_ORDINAL: u8 = 0u8;

#[derive(Debug)]
pub struct Ordinal {
    #[debug("PartitionHandle(\"{}\")", indices.name)]
    indices: PartitionHandle,
}

impl Ordinal {
    pub fn new(indices: PartitionHandle) -> Self {
        Self { indices }
    }
}

impl Append for Ordinal {
    fn append(&self, batch: &mut Batch, event: &(&[u8], [u8; 8]), index: [u8; 8]) {
        let mut key = BytesMut::with_capacity(17);
        key.put(&event.1[..]);
        key.put_u8(TYPED_ORDINAL);
        key.put(&index[..]);

        batch.insert(&self.indices, &key[..], []);
    }
}
