#![allow(clippy::multiple_crate_versions)]

use std::{
    error::Error,
    ops::Range,
    time::Instant,
};

use fjall::{
    Config,
    Keyspace,
    PartitionCreateOptions,
    UserKey,
};

const LEVEL_0: [u8; 1] = 0u8.to_be_bytes();
const LEVEL_1: [u8; 1] = 1u8.to_be_bytes();
const LEVEL_2: [u8; 1] = 2u8.to_be_bytes();

pub trait GetKey<const N: usize> {
    fn key(&self, path: [u16; N], position: u16) -> Key;
}

pub trait GetKeyRange<const N: usize> {
    fn range(&self, path: [u16; N], min: u16, max: u16) -> Range<Key>;
}

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

impl GetKey<0> for KeyFactory {
    fn key(&self, _: [u16; 0], position: u16) -> Key {
        Key {
            key: [&self.id[..], &LEVEL_0, &position.to_be_bytes()]
                .concat()
                .clone(),
        }
    }
}

impl GetKey<1> for KeyFactory {
    fn key(&self, path: [u16; 1], position: u16) -> Key {
        Key {
            key: [
                &self.id[..],
                &LEVEL_1,
                &path[0].to_be_bytes(),
                &position.to_be_bytes(),
            ]
            .concat()
            .clone(),
        }
    }
}

impl GetKey<2> for KeyFactory {
    fn key(&self, path: [u16; 2], position: u16) -> Key {
        Key {
            key: [
                &self.id[..],
                &LEVEL_2,
                &path[0].to_be_bytes(),
                &path[1].to_be_bytes(),
                &position.to_be_bytes(),
            ]
            .concat()
            .clone(),
        }
    }
}

impl GetKeyRange<0> for KeyFactory {
    fn range(&self, path: [u16; 0], min: u16, max: u16) -> Range<Key> {
        self.key(path, min)..self.key(path, max)
    }
}

impl GetKeyRange<1> for KeyFactory {
    fn range(&self, path: [u16; 1], min: u16, max: u16) -> Range<Key> {
        self.key(path, min)..self.key(path, max)
    }
}

impl GetKeyRange<2> for KeyFactory {
    fn range(&self, path: [u16; 2], min: u16, max: u16) -> Range<Key> {
        self.key(path, min)..self.key(path, max)
    }
}

pub struct Key {
    key: Vec<u8>,
}

impl From<Key> for UserKey {
    fn from(key: Key) -> Self {
        UserKey::from(key.key)
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}

pub struct KeyView<'a> {
    key: &'a UserKey,
}

impl<'a> KeyView<'a> {
    #[must_use]
    pub fn new(key: &'a UserKey) -> Self {
        Self { key }
    }
}

impl KeyView<'_> {
    #[must_use]
    pub fn id(&self) -> u16 {
        u16::from_be_bytes([self.key[0], self.key[1]])
    }

    #[must_use]
    pub fn level(&self) -> KeyLevelView<'_> {
        match (u8::from_be_bytes([self.key[2]]), self.key.len()) {
            (0, 5) => KeyLevelView::Level0 {
                level_0: KeyLevelPosition::<0>::new(self.key),
            },
            (1, 7) => KeyLevelView::Level1 {
                level_0: KeyLevelPosition::<0>::new(self.key),
                level_1: KeyLevelPosition::<1>::new(self.key),
            },
            (2, 9) => KeyLevelView::Level2 {
                level_0: KeyLevelPosition::<0>::new(self.key),
                level_1: KeyLevelPosition::<1>::new(self.key),
                level_2: KeyLevelPosition::<2>::new(self.key),
            },
            _ => unreachable!(),
        }
    }
}

pub enum KeyLevelView<'a> {
    Level0 {
        level_0: KeyLevelPosition<'a, 0>,
    },
    Level1 {
        level_0: KeyLevelPosition<'a, 0>,
        level_1: KeyLevelPosition<'a, 1>,
    },
    Level2 {
        level_0: KeyLevelPosition<'a, 0>,
        level_1: KeyLevelPosition<'a, 1>,
        level_2: KeyLevelPosition<'a, 2>,
    },
}

pub struct KeyLevelPosition<'a, const N: usize> {
    key: &'a UserKey,
}

impl<'a, const N: usize> KeyLevelPosition<'a, N> {
    fn new(key: &'a UserKey) -> Self {
        Self { key }
    }
}

impl KeyLevelPosition<'_, 0> {
    #[must_use]
    pub fn position(&self) -> u16 {
        u16::from_be_bytes([self.key[3], self.key[4]])
    }
}

impl KeyLevelPosition<'_, 1> {
    #[must_use]
    pub fn position(&self) -> u16 {
        u16::from_be_bytes([self.key[5], self.key[6]])
    }
}

impl KeyLevelPosition<'_, 2> {
    #[must_use]
    pub fn position(&self) -> u16 {
        u16::from_be_bytes([self.key[7], self.key[8]])
    }
}

pub fn main() -> Result<(), Box<dyn Error>> {
    let keyspace = Keyspace::open(Config::new("./keyspaces/test"))?;
    let idx_types = keyspace.open_partition("idx_types", PartitionCreateOptions::default())?;

    // keyspace.delete_partition(idx_types)?;
    // keyspace.persist(PersistMode::SyncAll)?;

    let type_a = KeyFactory::new(42);

    // idx_types.insert(type_a.position([], 0), "")?;
    // idx_types.insert(type_a.position([0], 0), "")?;
    // idx_types.insert(type_a.position([0, 0], 0), "")?;
    // idx_types.insert(type_a.position([0, 0], 13), "")?;
    // idx_types.insert(type_a.position([0, 0], 21), "")?;

    println!("idx len: {}", idx_types.approximate_len());

    let start = Instant::now();

    for kv in idx_types.range(type_a.range([0, 0], 0, 30)) {
        let (k, _) = kv?;
        let key = KeyView::new(&k);

        println!("k: {k:?}");
        println!("key: {}", key.id());

        if let KeyLevelView::Level2 { level_2, .. } = key.level() {
            println!("position: {}", level_2.position());
        }
    }

    let duration = Instant::now().duration_since(start);

    println!("duration: {duration:?}");

    Ok(())
}
