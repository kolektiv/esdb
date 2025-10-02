use std::{
    error::Error,
    time::Instant,
};

use bytes::{
    Buf as _,
    BufMut as _,
};
use musli::{
    Decode,
    Encode,
    Options,
    options::{
        self,
        ByteOrder,
    },
    packed::Encoding,
};

const OPTIONS: Options = options::new().fixed().byte_order(ByteOrder::Big).build();
const ENCODING: Encoding<OPTIONS> = Encoding::new().with_options();

pub fn main() -> Result<(), Box<dyn Error>> {
    let descriptor = 324_724_u64;
    let tags = [234_723_564_u64, 1231u64, 24878u64, 1_348_137_487u64]
        .into_iter()
        .collect::<Vec<_>>();
    let data = "this is some sample data".bytes().collect::<Vec<_>>();

    let test = TestStruct {
        descriptor,
        tags: tags.clone(),
        data: data.clone(),
    };

    let start = Instant::now();

    for _ in 0..1_000_000 {
        let mut encoded = Vec::new();

        ENCODING.encode(&mut encoded, &test)?;

        let _decoded: TestStruct = ENCODING.decode(&encoded[..])?;
    }

    let duration = Instant::now().duration_since(start);

    println!("musli: {duration:#?}");

    let start = Instant::now();

    for _ in 0..1_000_000 {
        let mut encoded = Vec::new();

        {
            encoded.put_u64(descriptor);
            encoded.put_u8(u8::try_from(tags.len()).expect("invalid tags"));

            for tag in &tags {
                encoded.put_u64(*tag);
            }

            encoded.put_slice(&data[..]);
        }

        let mut encoded = &encoded[..];

        let _descriptor = encoded.get_u64();

        let tags_len = encoded.get_u8();
        let mut tags = Vec::new();

        for _ in 0..tags_len {
            tags.push(encoded.get_u64());
        }

        let _data = encoded[..].iter().collect::<Vec<_>>();
    }

    let duration = Instant::now().duration_since(start);

    println!("buf: {duration:#?}");

    Ok(())
}

#[derive(Decode, Encode)]
#[musli(packed)]
pub struct TestStruct {
    descriptor: u64,
    tags: Vec<u64>,
    data: Vec<u8>,
}
