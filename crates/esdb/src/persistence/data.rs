use std::error::Error;

use bytes::{
    Buf,
    BufMut,
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
    persistence::{
        HashedEvent,
        POSITION_LEN,
    },
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
            Some((index, _)) => index.as_ref().get_u64() + 1,
            None => 0,
        };

        Ok(len)
    }
}

impl Data {
    pub fn insert(&self, batch: &mut Batch, position: Position, event: &HashedEvent) {
        let mut key = [0u8; POSITION_LEN];
        let mut value = Vec::new();

        {
            let mut key = &mut key[..];

            key.put_u64(position.value());

            value.put_u64(event.descriptor.hash());
            value.put_u8(event.descriptor.as_ref().version().value());
            value.put_u8(u8::try_from(event.tags.len()).expect("invalid tag count"));

            for tag in &event.tags {
                value.put_u64(tag.hash());
            }

            value.put_slice(&event.data);
        }

        batch.insert(&self.data, key, value);
    }
}
