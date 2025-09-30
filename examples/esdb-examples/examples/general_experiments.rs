use std::error::Error;

use bytes::{
    BufMut as _,
    BytesMut,
};

pub fn main() -> Result<(), Box<dyn Error>> {
    let mut keys = Vec::new();
    let mut key = BytesMut::with_capacity(18);

    key.put_u8(0);
    key.put_u64(0);
    key.put_u8(0);
    key.put_u64(234);

    keys.push(key.to_vec());

    key.clear();
    key.put_u8(0);
    key.put_u64(1);
    key.put_u8(2);
    key.put_u64(234);

    keys.push(key.to_vec());

    key.clear();
    key.put_u8(0);
    key.put_u64(2);
    key.put_u8(1);
    key.put_u64(234);

    keys.push(key.to_vec());

    println!("keys: {keys:?}");

    keys.sort_unstable();

    println!("keys sorted: {keys:?}");

    key.clear();
    key.put_u8(0);
    key.put_u64(0);
    key.put_u8(0);
    key.put_u64(234);

    let lower = key.to_vec();

    key.clear();
    key.put_u8(0);
    key.put_u64(u64::MAX);
    key.put_u8(1);
    key.put_u64(234);

    let upper = key.to_vec();

    let range = lower..upper;

    keys.iter()
        .filter(|k| range.contains(*k))
        .for_each(|k| println!("k: {k:?}"));

    Ok(())
}
