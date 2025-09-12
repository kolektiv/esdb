#![allow(clippy::multiple_crate_versions)]

use std::{
    error::Error,
    ops::Range,
};

use fjall::{
    Config,
    Keyspace,
    PartitionCreateOptions,
    UserKey,
};

// -------------------------------------------------------------------------------------------------

// Traits

// Keys

pub trait GetLevel0Key {
    fn level_0_key(&self, position_0: u32) -> Key;
}

pub trait GetLevel1Key {
    fn level_1_key(&self, position_0: u32, position_1: u8) -> Key;
}

pub trait GetLevel2Key {
    fn level_2_key(&self, position_0: u32, position_1: u8, position_2: u8) -> Key;
}

// Ranges

pub trait GetLevel0Range {
    fn level_0_range(&self, min: u32, max: u32) -> Range<Key>;
}

pub trait GetLevel1Range {
    fn level_1_range(&self, position_0: u32, min: u8, max: u8) -> Range<Key>;
}

pub trait GetLevel2Range {
    fn level_2_range(&self, position_0: u32, position_1: u8, min: u8, max: u8) -> Range<Key>;
}

// -------------------------------------------------------------------------------------------------

// Key Factory

const LEVEL_0: [u8; 1] = 0u8.to_be_bytes();
const LEVEL_1: [u8; 1] = 1u8.to_be_bytes();
const LEVEL_2: [u8; 1] = 2u8.to_be_bytes();

pub struct KeyFactory {
    id: [u8; 2],
}

impl KeyFactory {
    #[must_use]
    pub fn new(id: u16) -> Self {
        let id = id.to_be_bytes();

        Self { id }
    }
}

impl GetLevel0Key for KeyFactory {
    fn level_0_key(&self, position_0: u32) -> Key {
        Key::new([&self.id[..], &LEVEL_0, &position_0.to_be_bytes()].concat())
    }
}

impl GetLevel1Key for KeyFactory {
    fn level_1_key(&self, position_0: u32, position_1: u8) -> Key {
        Key::new(
            [
                &self.id[..],
                &LEVEL_1,
                &position_0.to_be_bytes(),
                &position_1.to_be_bytes(),
            ]
            .concat(),
        )
    }
}

impl GetLevel2Key for KeyFactory {
    fn level_2_key(&self, position_0: u32, position_1: u8, position_2: u8) -> Key {
        Key::new(
            [
                &self.id[..],
                &LEVEL_2,
                &position_0.to_be_bytes(),
                &position_1.to_be_bytes(),
                &position_2.to_be_bytes(),
            ]
            .concat(),
        )
    }
}

impl GetLevel0Range for KeyFactory {
    fn level_0_range(&self, min: u32, max: u32) -> Range<Key> {
        self.level_0_key(min)..self.level_0_key(max)
    }
}

impl GetLevel1Range for KeyFactory {
    fn level_1_range(&self, position_0: u32, min: u8, max: u8) -> Range<Key> {
        self.level_1_key(position_0, min)..self.level_1_key(position_0, max)
    }
}

impl GetLevel2Range for KeyFactory {
    fn level_2_range(&self, position_0: u32, position_1: u8, min: u8, max: u8) -> Range<Key> {
        self.level_2_key(position_0, position_1, min)..self.level_2_key(position_0, position_1, max)
    }
}

// -------------------------------------------------------------------------------------------------

// Key

pub struct Key {
    key: Vec<u8>,
}

impl Key {
    fn new(key: impl Into<Vec<u8>>) -> Self {
        let key = key.into();

        Self { key }
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}

impl From<Key> for UserKey {
    fn from(key: Key) -> Self {
        UserKey::from(key.key)
    }
}

// pub struct KeyView<'a> {
//     key: &'a UserKey,
// }

// impl<'a> KeyView<'a> {
//     #[must_use]
//     pub fn new(key: &'a UserKey) -> Self {
//         Self { key }
//     }
// }

// impl KeyView<'_> {
//     #[must_use]
//     pub fn id(&self) -> u16 {
//         u16::from_be_bytes([self.key[0], self.key[1]])
//     }

//     #[must_use]
//     pub fn level(&self) -> KeyLevelView<'_> {
//         match (u8::from_be_bytes([self.key[2]]), self.key.len()) {
//             (0, 5) => KeyLevelView::Level0 {
//                 level_0: KeyLevelPosition::<0>::new(self.key),
//             },
//             (1, 7) => KeyLevelView::Level1 {
//                 level_0: KeyLevelPosition::<0>::new(self.key),
//                 level_1: KeyLevelPosition::<1>::new(self.key),
//             },
//             (2, 9) => KeyLevelView::Level2 {
//                 level_0: KeyLevelPosition::<0>::new(self.key),
//                 level_1: KeyLevelPosition::<1>::new(self.key),
//                 level_2: KeyLevelPosition::<2>::new(self.key),
//             },
//             _ => unreachable!(),
//         }
//     }
// }

// pub enum KeyLevelView<'a> {
//     Level0 {
//         level_0: KeyLevelPosition<'a, 0>,
//     },
//     Level1 {
//         level_0: KeyLevelPosition<'a, 0>,
//         level_1: KeyLevelPosition<'a, 1>,
//     },
//     Level2 {
//         level_0: KeyLevelPosition<'a, 0>,
//         level_1: KeyLevelPosition<'a, 1>,
//         level_2: KeyLevelPosition<'a, 2>,
//     },
// }

// pub struct KeyLevelPosition<'a, const N: usize> {
//     key: &'a UserKey,
// }

// impl<'a, const N: usize> KeyLevelPosition<'a, N> {
//     fn new(key: &'a UserKey) -> Self {
//         Self { key }
//     }
// }

// impl KeyLevelPosition<'_, 0> {
//     #[must_use]
//     pub fn position(&self) -> u16 {
//         u16::from_be_bytes([self.key[3], self.key[4]])
//     }
// }

// impl KeyLevelPosition<'_, 1> {
//     #[must_use]
//     pub fn position(&self) -> u16 {
//         u16::from_be_bytes([self.key[5], self.key[6]])
//     }
// }

// impl KeyLevelPosition<'_, 2> {
//     #[must_use]
//     pub fn position(&self) -> u16 {
//         u16::from_be_bytes([self.key[7], self.key[8]])
//     }
// }

pub fn main() -> Result<(), Box<dyn Error>> {
    let keyspace = Keyspace::open(Config::new("./keyspaces/test"))?;
    let idx_types = keyspace.open_partition("idx_types", PartitionCreateOptions::default())?;

    // keyspace.delete_partition(idx_types)?;
    // keyspace.persist(PersistMode::SyncAll)?;

    let type_a = KeyFactory::new(42);

    idx_types.insert(type_a.level_0_key(0), "")?;
    idx_types.insert(type_a.level_0_key(564), "")?;
    idx_types.insert(type_a.level_1_key(0, 0), "")?;
    idx_types.insert(type_a.level_2_key(0, 0, 0), "")?;
    idx_types.insert(type_a.level_2_key(0, 0, 13), "")?;
    idx_types.insert(type_a.level_2_key(0, 0, 216), "")?;

    println!("idx len: {}", idx_types.approximate_len());

    // let start = Instant::now();

    for kv in idx_types.iter() {
        let (k, _) = kv?;
        // let key = KeyView::new(&k);

        println!("k: {k:?}");
        // println!("key: {}", key.id());

        // if let KeyLevelView::Level2 { level_2, .. } = key.level() {
        //     println!("position: {}", level_2.position());
        // }
    }

    // let duration = Instant::now().duration_since(start);

    // println!("duration: {duration:?}");

    Ok(())
}
