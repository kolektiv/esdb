use std::error::Error;

use bitvec::{
    array::BitArray,
    order::Msb0,
};

pub fn main() -> Result<(), Box<dyn Error>> {
    let mut a1: BitArray<[u8; 1], Msb0> = BitArray::new([0u8]);

    unsafe {
        *a1.get_unchecked_mut(3) = true;
        *a1.get_unchecked_mut(5) = true;
    }

    println!("a1: {a1:?}");

    let mut a2: BitArray<[u8; 1], Msb0> = BitArray::new([0u8]);

    unsafe {
        *a2.get_unchecked_mut(1) = true;
        *a2.get_unchecked_mut(7) = true;
    }

    println!("a2: {a2:?}");

    let arr3 = a1 | a2;

    println!("a3: {arr3:?}");

    arr3.iter_ones().for_each(|i| println!("{i}"));

    Ok(())
}
