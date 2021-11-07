//! Storage side of database implementation. Currently using sled.
//! Everything handling the serialization side of things should be
//! handled in db/mod.rs or db/tree.rs.

use std::path::Path;

pub use super::storage_tree::InnerTree;

#[derive(Debug, Clone)]
pub struct InnerDb {
    db: sled::Db,
}

impl InnerDb {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }

    pub fn open(path: &Path) -> Result<Self, sled::Error> {
        sled::open(path).map(Self::new)
    }

    pub fn tree(&self, name: &[u8]) -> InnerTree {
        let tree = self.db.open_tree(name).unwrap();
        InnerTree { tree }
    }
}

// /// Serialize a struct into rkyv format, tag the position at the end, and return as a Vec<u8>.
// pub fn serialize_rkyv<T: Serialize<WriteSerializer<Vec<u8>>>>(data: &T) -> Vec<u8> {
//     let mut serializer = WriteSerializer::new(Vec::new());
//     let pos = serializer.serialize_value(data).unwrap();

//     let mut out = serializer.into_inner();
//     out.extend_from_slice(&pos.to_ne_bytes());
//     out
// }

// /// Read the position from the end of a slice, and use it to get the archive'd variant of a struct.
// pub fn get_rkyv<T: Archive>(data: &[u8]) -> &'_ T::Archived {
//     let tag_pos = data.len().saturating_sub(size_of::<usize>());
//     let pos = usize::from_ne_bytes(data[tag_pos..].try_into().unwrap());
//     unsafe { archived_value::<T>(&data[..tag_pos], pos) }
// }

// /// Read the position from the end of a slice, and use it to get the mutable archive'd variant of a struct.
// pub fn get_rkyv_mut<T: Archive>(data: &mut [u8]) -> Pin<&'_ mut T::Archived> {
//     let tag_pos = data.len().saturating_sub(size_of::<usize>());
//     let pos = usize::from_ne_bytes(data[tag_pos..].try_into().unwrap());
//     let data_pin = Pin::new(&mut data[..tag_pos]);
//     unsafe { archived_value_mut::<T>(data_pin, pos) }
// }

// #[cfg(test)]
// mod tests {
//     use rkyv::{de::deserializers::AllocDeserializer, Deserialize};

//     use super::*;
//     use crate::types::DataIdentifier;

//     #[derive(Clone, Debug, PartialEq, Archive, Serialize)]
//     struct TestStruct {
//         data: Vec<u8>,
//         num: u64,
//         flag: bool,
//     }

//     #[test]
//     fn test_db_serialization_1() {
//         let s = TestStruct {
//             data: vec![b'A'; 8],
//             num: 123456,
//             flag: true,
//         };

//         let ss = serialize_rkyv(&s);

//         dbg!(&ss);

//         let sd = get_rkyv::<TestStruct>(&ss);
//         dbg!(&*sd.data);
//         dbg!(&sd.num);
//         dbg!(&sd.flag);

//         assert_eq!(s.data, *sd.data);
//         assert_eq!(s.num, sd.num);
//         assert_eq!(s.flag, sd.flag);
//     }

//     #[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
//     struct TestStruct2 {
//         data_id: DataIdentifier,
//         num: u64,
//         flag: bool,
//     }

//     #[test]
//     fn test_db_serialization_2() {
//         let s = TestStruct2 {
//             data_id: DataIdentifier::GlobalMd5(vec![b'A'; 8]),
//             num: 123456,
//             flag: true,
//         };

//         let ss = serialize_rkyv(&s);

//         dbg!(&ss);

//         let sd = get_rkyv::<TestStruct2>(&ss);
//         let sd_data_id = sd.data_id.deserialize(&mut AllocDeserializer).unwrap();
//         dbg!(&sd_data_id);
//         dbg!(&sd.num);
//         dbg!(&sd.flag);

//         assert_eq!(s.data_id, sd_data_id);
//         assert_eq!(s.num, sd.num);
//         assert_eq!(s.flag, sd.flag);
//     }

//     #[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
//     struct TestStruct3 {
//         data_slice: [u8; 9],
//         num: u64,
//         flag: bool,
//     }

//     #[test]
//     fn test_db_serialization_3() {
//         let new_data_slice = [9u8; 9];

//         let s = TestStruct3 {
//             data_slice: [0u8; 9],
//             num: 123456,
//             flag: true,
//         };

//         let mut ss = serialize_rkyv(&s);

//         dbg!(&ss);

//         {
//             let mut smd = get_rkyv_mut::<TestStruct3>(&mut ss);
//             smd.data_slice = new_data_slice;
//         }

//         let sd = get_rkyv::<TestStruct3>(&ss);
//         dbg!(&sd.data_slice);
//         dbg!(&sd.num);
//         dbg!(&sd.flag);

//         assert_eq!(new_data_slice, sd.data_slice);
//         assert_eq!(s.num, sd.num);
//         assert_eq!(s.flag, sd.flag);
//     }
// }
