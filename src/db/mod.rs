use std::{convert::TryInto, mem::size_of, path::Path, pin::Pin};

use rkyv::{
    archived_value, archived_value_mut,
    ser::{serializers::WriteSerializer, Serializer},
    Archive, Serialize,
};
use sled::{IVec, Iter};

#[derive(Clone)]
pub struct Db {
    db: sled::Db,
}

impl Db {
    pub fn new(db: sled::Db) -> Db {
        Db { db }
    }

    pub fn open(path: &Path) -> Result<Db, sled::Error> {
        sled::open(path).map(|db| Self::new(db))
    }

    pub fn tree(&self, name: &[u8]) -> Tree {
        let tree = self.db.open_tree(name).unwrap();
        Tree { tree }
    }
}

#[derive(Clone)]
pub struct Tree {
    pub tree: sled::Tree,
}

impl Tree {
    /// Get a value from the DB tree.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<IVec> {
        self.tree.get(key).unwrap()
    }

    /// Get a value into the DB tree.
    pub fn insert<K, V>(&self, key: K, value: V) -> Option<IVec>
    where
        K: AsRef<[u8]>,
        V: Into<IVec>,
    {
        println!("db insert");
        self.tree.insert(key, value).unwrap()
    }

    /// Remove a value from the DB tree.
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Option<IVec> {
        self.tree.remove(key).unwrap()
    }

    /// Compare and swap values in the DB tree.
    pub fn compare_and_swap<K, OV, NV>(&self, key: K, old: Option<OV>, new: Option<NV>) -> bool
    where
        K: AsRef<[u8]>,
        OV: AsRef<[u8]>,
        NV: Into<IVec>,
    {
        Ok(Ok(())) == self.tree.compare_and_swap(key, old, new)
    }

    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    pub fn clear(&self) {
        self.tree.clear().unwrap()
    }

    pub fn iter(&self) -> Iter {
        self.tree.iter()
    }
}

/// Serialize a struct into rkyv format, tag the position at the end, and return as a Vec<u8>.
pub fn serialize_rkyv<T: Serialize<WriteSerializer<Vec<u8>>>>(data: &T) -> Vec<u8> {
    let mut serializer = WriteSerializer::new(Vec::new());
    let pos = serializer.serialize_value(data).unwrap();

    let mut out = serializer.into_inner();
    out.extend_from_slice(&pos.to_ne_bytes());
    out
}

/// Read the position from the end of a slice, and use it to get the archive'd variant of a struct.
pub fn get_rkyv<'a, T: Archive>(data: &'a [u8]) -> &'a T::Archived {
    let tag_pos = data.len().saturating_sub(size_of::<usize>());
    let pos = usize::from_ne_bytes(data[tag_pos..].try_into().unwrap());
    unsafe { archived_value::<T>(&data[..tag_pos], pos) }
}

/// Read the position from the end of a slice, and use it to get the mutable archive'd variant of a struct.
pub fn get_rkyv_mut<'a, T: Archive>(data: &'a mut [u8]) -> Pin<&'a mut T::Archived> {
    let tag_pos = data.len().saturating_sub(size_of::<usize>());
    let pos = usize::from_ne_bytes(data[tag_pos..].try_into().unwrap());
    let data_pin = Pin::new(&mut data[..tag_pos]);
    unsafe { archived_value_mut::<T>(data_pin, pos) }
}

#[cfg(test)]
mod tests {
    use rkyv::de::deserializers::AllocDeserializer;
    use rkyv::Deserialize;

    use crate::types::DataIdentifier;

    use super::*;

    #[derive(Clone, Debug, PartialEq, Archive, Serialize)]
    struct TestStruct {
        data: Vec<u8>,
        num: u64,
        flag: bool,
    }

    #[test]
    fn test_db_serialization_1() {
        let s = TestStruct {
            data: vec![b'A'; 8],
            num: 123456,
            flag: true,
        };

        let ss = serialize_rkyv(&s);

        dbg!(&ss);

        let sd = get_rkyv::<TestStruct>(&ss);
        dbg!(&*sd.data);
        dbg!(&sd.num);
        dbg!(&sd.flag);

        assert_eq!(s.data, *sd.data);
        assert_eq!(s.num, sd.num);
        assert_eq!(s.flag, sd.flag);
    }

    #[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
    struct TestStruct2 {
        data_id: DataIdentifier,
        num: u64,
        flag: bool,
    }

    #[test]
    fn test_db_serialization_2() {
        let s = TestStruct2 {
            data_id: DataIdentifier::GlobalMd5(vec![b'A'; 8]),
            num: 123456,
            flag: true,
        };

        let ss = serialize_rkyv(&s);

        dbg!(&ss);

        let sd = get_rkyv::<TestStruct2>(&ss);
        let sd_data_id = sd.data_id.deserialize(&mut AllocDeserializer).unwrap();
        dbg!(&sd_data_id);
        dbg!(&sd.num);
        dbg!(&sd.flag);

        assert_eq!(s.data_id, sd_data_id);
        assert_eq!(s.num, sd.num);
        assert_eq!(s.flag, sd.flag);
    }

    #[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
    struct TestStruct3 {
        data_slice: [u8; 9],
        num: u64,
        flag: bool,
    }

    #[test]
    fn test_db_serialization_3() {
        let new_data_slice = [9u8; 9];

        let s = TestStruct3 {
            data_slice: [0u8; 9],
            num: 123456,
            flag: true,
        };

        let mut ss = serialize_rkyv(&s);

        dbg!(&ss);

        {
            let mut smd = get_rkyv_mut::<TestStruct3>(&mut ss);
            smd.data_slice = new_data_slice;
        }

        let sd = get_rkyv::<TestStruct3>(&ss);
        dbg!(&sd.data_slice);
        dbg!(&sd.num);
        dbg!(&sd.flag);

        assert_eq!(new_data_slice, sd.data_slice);
        assert_eq!(s.num, sd.num);
        assert_eq!(s.flag, sd.flag);
    }
}
