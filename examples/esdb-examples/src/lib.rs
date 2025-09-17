#![allow(clippy::multiple_crate_versions)]

use std::error::Error;

use musli::{
    Decode,
    Encode,
    IntoReader,
    IntoWriter,
    Options,
    options::{
        self,
        ByteOrder,
    },
    packed::Encoding,
};

const OPTIONS: Options = options::new().byte_order(ByteOrder::Big).variable().build();
const ENCODING: Encoding<OPTIONS> = Encoding::new().with_options();

#[derive(Debug, Decode, Encode)]
#[musli(packed)]
pub struct EphemeralStorageEvent {
    pub data: Vec<u8>,
    pub tag_values: Vec<String>,
    pub type_id: u16,
}

impl EphemeralStorageEvent {
    /// .
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub fn decode<'de, R>(reader: R) -> Result<Self, Box<dyn Error>>
    where
        R: IntoReader<'de>,
    {
        let value = ENCODING.decode(reader)?;

        Ok(value)
    }

    /// .
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub fn encode<W>(&self, writer: W) -> Result<(), Box<dyn Error>>
    where
        W: IntoWriter,
    {
        ENCODING.encode(writer, &self)?;

        Ok(())
    }
}
