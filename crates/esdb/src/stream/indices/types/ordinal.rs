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

use crate::stream::Insert;

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
    pub fn view(&self, type_tag: u64) -> OrdinalView {
        OrdinalView::new(self.indices.clone(), type_tag)
    }
}

impl Insert for Ordinal {
    fn insert(&self, batch: &mut Batch, event: &(&[u8], u64), position: u64) {
        let mut key = BytesMut::with_capacity(17);

        key.put_u64(event.1);
        key.put_u8(TYPED_ORDINAL);
        key.put_u64(position);

        batch.insert(&self.indices, &key[..], []);
    }
}

#[derive(Debug)]
pub struct OrdinalView {
    #[debug("PartitionHandle(\"{}\")", indices.name)]
    indices: PartitionHandle,
    type_tag: u64,
}

impl OrdinalView {
    fn new(indices: PartitionHandle, type_tag: u64) -> Self {
        Self { indices, type_tag }
    }
}

impl OrdinalView {
    pub fn positioned(&self, position: u64) -> OrdinalViewPositioned {
        OrdinalViewPositioned::new(self.indices.clone(), position, self.type_tag)
    }
}

impl IntoIterator for OrdinalView {
    type IntoIter = Box<dyn Iterator<Item = u64>>;
    type Item = u64;

    fn into_iter(self) -> Self::IntoIter {
        let mut prefix = BytesMut::with_capacity(9);

        prefix.put_u64(self.type_tag);
        prefix.put_u8(TYPED_ORDINAL);

        Box::new(self.indices.prefix(prefix).map(map_kv))
    }
}

#[derive(Debug)]
pub struct OrdinalViewPositioned {
    #[debug("PartitionHandle(\"{}\")", indices.name)]
    indices: PartitionHandle,
    position: u64,
    type_tag: u64,
}

impl OrdinalViewPositioned {
    fn new(indices: PartitionHandle, position: u64, type_tag: u64) -> Self {
        Self {
            indices,
            position,
            type_tag,
        }
    }
}

impl IntoIterator for OrdinalViewPositioned {
    type IntoIter = Box<dyn Iterator<Item = u64>>;
    type Item = u64;

    fn into_iter(self) -> Self::IntoIter {
        let mut lower = BytesMut::with_capacity(17);

        lower.put_u64(self.type_tag);
        lower.put_u8(TYPED_ORDINAL);
        lower.put_u64(self.position);

        let mut upper = BytesMut::with_capacity(17);

        upper.put_u64(self.type_tag);
        upper.put_u8(TYPED_ORDINAL);
        upper.put_u64(u64::MAX);

        Box::new(self.indices.range(lower..upper).map(map_kv))
    }
}

fn map_kv(kv: Result<(Slice, Slice), Error>) -> u64 {
    let mut k = &kv.expect("invalid index iteration").0[..];

    k.advance(9);
    k.get_u64()
}
