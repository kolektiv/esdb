use std::error::Error;

use bytes::Buf;
use derive_more::Debug;
use fjall::{
    Batch,
    PartitionCreateOptions,
    PartitionHandle,
};

use crate::data::Data;

static EVENTS: &str = "events";

pub trait Write {
    fn write(&self, batch: &mut Batch, event: &[u8], position: u64);
}

#[derive(Debug)]
pub struct Events {
    #[debug("PartitionHandle(\"{}\")", events.name)]
    events: PartitionHandle,
}

impl Events {
    pub fn new(data: &Data) -> Result<Self, Box<dyn Error>> {
        let events_options = PartitionCreateOptions::default();
        let events = data.keyspace.open_partition(EVENTS, events_options)?;

        Ok(Self { events })
    }
}

impl Events {
    pub fn is_empty(&self) -> Result<bool, Box<dyn Error>> {
        self.len().map(|len| len == 0)
    }

    pub fn len(&self) -> Result<u64, Box<dyn Error>> {
        let last = self.events.last_key_value()?;
        let len = match last {
            Some((index, _)) => index.as_ref().get_u64() + 1,
            None => 0,
        };

        Ok(len)
    }
}

impl Write for Events {
    fn write(&self, batch: &mut Batch, event: &[u8], position: u64) {
        batch.insert(&self.events, position.to_be_bytes(), event);
    }
}
