use std::{
    error::Error,
    path::Path,
};

use derive_more::Debug;
use fjall::{
    Batch,
    Config,
    Keyspace,
};

#[derive(Debug)]
pub struct Data {
    #[debug("Keyspace")]
    pub(super) keyspace: Keyspace,
}

impl Data {
    pub fn new<P>(path: P) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let keyspace = Config::new(path).open()?;

        Ok(Self { keyspace })
    }
}

impl Data {
    #[must_use]
    pub fn batch(&self) -> Batch {
        self.keyspace.batch()
    }
}

impl AsRef<Keyspace> for Data {
    fn as_ref(&self) -> &Keyspace {
        &self.keyspace
    }
}
