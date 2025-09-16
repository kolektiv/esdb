#![allow(clippy::multiple_crate_versions)]

use std::{
    error::Error,
    ops::Range,
};

use fjall::{
    Config,
    Keyspace,
    PartitionCreateOptions,
    Slice,
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

// Views

pub trait GetId {
    fn id(&self) -> u16;
}

pub trait GetLevel0Position {
    fn level_0_position(&self) -> u32;
}

pub trait GetLevel1Position {
    fn level_1_position(&self) -> u8;
}

pub trait GetLevel2Position {
    fn level_2_position(&self) -> u8;
}

// -------------------------------------------------------------------------------------------------

// Key Factory

const LEVEL_0: [u8; 1] = 0u8.to_be_bytes();
const LEVEL_1: [u8; 1] = 1u8.to_be_bytes();
const LEVEL_2: [u8; 1] = 2u8.to_be_bytes();

#[derive(Debug)]
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

#[derive(Debug)]
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

impl From<Key> for Slice {
    fn from(key: Key) -> Self {
        Slice::from(key.key)
    }
}

// -------------------------------------------------------------------------------------------------

// Key View

#[derive(Debug)]
pub enum KeyView<'a> {
    Level0(KeyViewLevel0<'a>),
    Level1(KeyViewLevel1<'a>),
    Level2(KeyViewLevel2<'a>),
}

impl<'a> KeyView<'a> {
    #[must_use]
    pub fn new(slice: &'a Slice) -> Self {
        match [slice[2]] {
            LEVEL_0 => Self::Level0(KeyViewLevel0 { slice }),
            LEVEL_1 => Self::Level1(KeyViewLevel1 { slice }),
            LEVEL_2 => Self::Level2(KeyViewLevel2 { slice }),
            _ => unreachable!(),
        }
    }
}

impl GetId for KeyView<'_> {
    #[rustfmt::skip]
    fn id(&self) -> u16 {
        match self {
            Self::Level0(KeyViewLevel0 { slice }) |
            Self::Level1(KeyViewLevel1 { slice }) |
            Self::Level2(KeyViewLevel2 { slice }) => id(slice),
        }
    }
}

#[derive(Debug)]
pub struct KeyViewLevel0<'a> {
    slice: &'a Slice,
}

impl GetLevel0Position for KeyViewLevel0<'_> {
    fn level_0_position(&self) -> u32 {
        level_0_position(self.slice)
    }
}

#[derive(Debug)]
pub struct KeyViewLevel1<'a> {
    slice: &'a Slice,
}

impl GetLevel0Position for KeyViewLevel1<'_> {
    fn level_0_position(&self) -> u32 {
        level_0_position(self.slice)
    }
}

impl GetLevel1Position for KeyViewLevel1<'_> {
    fn level_1_position(&self) -> u8 {
        level_1_position(self.slice)
    }
}

#[derive(Debug)]
pub struct KeyViewLevel2<'a> {
    slice: &'a Slice,
}

impl GetLevel0Position for KeyViewLevel2<'_> {
    fn level_0_position(&self) -> u32 {
        level_0_position(self.slice)
    }
}

impl GetLevel1Position for KeyViewLevel2<'_> {
    fn level_1_position(&self) -> u8 {
        level_1_position(self.slice)
    }
}

impl GetLevel2Position for KeyViewLevel2<'_> {
    fn level_2_position(&self) -> u8 {
        level_2_position(self.slice)
    }
}

fn id(slice: &Slice) -> u16 {
    u16::from_be_bytes([slice[0], slice[1]])
}

fn level_0_position(slice: &Slice) -> u32 {
    u32::from_be_bytes([slice[3], slice[4], slice[5], slice[6]])
}

fn level_1_position(slice: &Slice) -> u8 {
    u8::from_be_bytes([slice[7]])
}

fn level_2_position(slice: &Slice) -> u8 {
    u8::from_be_bytes([slice[8]])
}

// -------------------------------------------------------------------------------------------------

// Main

pub fn main() -> Result<(), Box<dyn Error>> {
    let keyspace = Keyspace::open(Config::new("./keyspaces/test"))?;
    let idx_types = keyspace.open_partition("idx_types", PartitionCreateOptions::default())?;

    // keyspace.delete_partition(idx_types)?;
    // keyspace.persist(PersistMode::SyncAll)?;

    // let type_a = KeyFactory::new(42);

    // idx_types.insert(type_a.level_0_key(0), "")?;
    // idx_types.insert(type_a.level_0_key(564), "")?;
    // idx_types.insert(type_a.level_1_key(0, 0), "")?;
    // idx_types.insert(type_a.level_2_key(0, 0, 0), "")?;
    // idx_types.insert(type_a.level_2_key(0, 0, 13), "")?;
    // idx_types.insert(type_a.level_2_key(0, 0, 216), "")?;

    // let start = Instant::now();

    for kv in idx_types.iter() {
        let (k, _) = kv?;
        let key = KeyView::new(&k);

        println!("k: {k:?}");
        println!("key: {key:#?}");

        // if let KeyLevelView::Level2 { level_2, .. } = key.level() {
        //     println!("position: {}", level_2.position());
        // }
    }

    // let duration = Instant::now().duration_since(start);

    // println!("duration: {duration:?}");

    Ok(())
}
