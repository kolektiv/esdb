use std::error::Error;

use bytes::{
    Buf as _,
    BufMut as _,
};
use derive_more::Debug;
use fjall::{
    Batch,
    Keyspace,
    PartitionCreateOptions,
    PartitionHandle,
};

use crate::{
    model::Position,
    persistence::HashedEvent,
};

// =================================================================================================
// Data
// =================================================================================================

// Configuration

static DATA_PARTITION_NAME: &str = "data";

// -------------------------------------------------------------------------------------------------

// Data

#[derive(Debug)]
pub struct Data {
    #[debug("PartitionHandle(\"{}\")", data.name)]
    data: PartitionHandle,
}

impl Data {
    pub fn new(keyspace: &Keyspace) -> Result<Self, Box<dyn Error>> {
        let data_name = DATA_PARTITION_NAME;
        let data_options = PartitionCreateOptions::default();
        let data = keyspace.open_partition(data_name, data_options)?;

        Ok(Self { data })
    }
}

impl Data {
    pub fn is_empty(&self) -> Result<bool, Box<dyn Error>> {
        self.len().map(|len| len == 0)
    }

    pub fn len(&self) -> Result<u64, Box<dyn Error>> {
        let last = self.data.last_key_value()?;
        let len = match last {
            Some((key, _)) => key.as_ref().get_u64() + 1,
            None => 0,
        };

        Ok(len)
    }
}

impl Data {
    pub fn insert(&self, batch: &mut Batch, position: Position, event: &HashedEvent) {
        let key = position.value().to_be_bytes();
        let mut value = Vec::new();

        get_data_value(&mut value, event);

        batch.insert(&self.data, key, value);
    }
}

fn get_data_value(value: &mut Vec<u8>, event: &HashedEvent) {
    let descriptor_identifier = event.descriptor.hash();
    let descriptor_version = event.descriptor.as_ref().version().value();
    let tags_len = u8::try_from(event.tags.len()).expect("max tag count exceeded");

    value.put_u64(descriptor_identifier);
    value.put_u8(descriptor_version);
    value.put_u8(tags_len);

    for tag in &event.tags {
        let tag = tag.hash();

        value.put_u64(tag);
    }

    let data = &event.data;

    value.put_slice(data);
}
