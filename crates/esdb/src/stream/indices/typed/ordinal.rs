use bytes::{
    Buf,
    BufMut,
    BytesMut,
};
use derive_more::Debug;
use fjall::{
    Batch,
    Error,
    PartitionHandle,
    Slice,
};

use crate::stream::Append;

static TYPED_ORDINAL: u8 = 0;

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

impl Ordinal {
    pub fn iter(&self, type_tag: u64) -> impl DoubleEndedIterator<Item = u64> {
        let mut prefix = BytesMut::with_capacity(9);

        prefix.put_u64(type_tag);
        prefix.put_u8(TYPED_ORDINAL);

        self.indices.prefix(prefix).map(map_kv)
    }

    pub fn positioned(&self, position: u64) -> OrdinalPositioned {
        OrdinalPositioned::new(self.indices.clone(), position)
    }
}

impl Append for Ordinal {
    fn append(&self, batch: &mut Batch, event: &(&[u8], u64), position: u64) {
        let mut key = BytesMut::with_capacity(17);

        key.put_u64(event.1);
        key.put_u8(TYPED_ORDINAL);
        key.put_u64(position);

        batch.insert(&self.indices, &key[..], []);
    }
}

#[derive(Debug)]
pub struct OrdinalPositioned {
    #[debug("PartitionHandle(\"{}\")", indices.name)]
    indices: PartitionHandle,
    position: u64,
}

impl OrdinalPositioned {
    fn new(indices: PartitionHandle, position: u64) -> Self {
        Self { indices, position }
    }
}

impl OrdinalPositioned {
    pub fn iter(&self, type_tag: u64) -> impl DoubleEndedIterator<Item = u64> {
        let mut lower = BytesMut::with_capacity(17);

        lower.put_u64(type_tag);
        lower.put_u8(TYPED_ORDINAL);
        lower.put_u64(self.position);

        let mut upper = BytesMut::with_capacity(17);

        upper.put_u64(type_tag);
        upper.put_u8(TYPED_ORDINAL);
        upper.put_u64(u64::MAX);

        self.indices.range(lower..upper).map(map_kv)
    }
}

fn map_kv(kv: Result<(Slice, Slice), Error>) -> u64 {
    let mut k = &kv.expect("invalid index iteration").0[..];

    k.advance(9);
    k.get_u64()
}
