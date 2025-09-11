use std::{
    collections::HashSet,
    error::Error,
    mem,
};

use fastset::Set;
use rand::Rng;

pub fn main() -> Result<(), Box<dyn Error>> {
    const N: usize = 100_000;
    const S: usize = 1_000;

    let mut rng = rand::rng();

    let mut std_hash = HashSet::new();
    let mut fastset_set = Set::with_max(100_000);

    println!(
        "size of std_hash: {} (len: {}/cap: {})",
        std_hash.capacity() * mem::size_of::<usize>() + mem::size_of_val(&std_hash),
        std_hash.len(),
        std_hash.capacity(),
    );

    println!(
        "size of fastset_set: {} (len: {})",
        mem::size_of_val(&fastset_set),
        fastset_set.len()
    );

    for _ in 0..S {
        let i = rng.random_range(0..N);

        std_hash.insert(i);
        fastset_set.insert(i);
    }

    println!(
        "size of std_hash: {} (len: {}/cap: {})",
        std_hash.capacity() * mem::size_of::<usize>() + mem::size_of_val(&std_hash),
        std_hash.len(),
        std_hash.capacity(),
    );

    println!(
        "size of fastset_set: {} (len: {})",
        mem::size_of_val(&fastset_set),
        fastset_set.len()
    );

    Ok(())
}
