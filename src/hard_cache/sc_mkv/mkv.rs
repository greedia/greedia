// Copyright 2017-2020 Brian Langenberger
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// This file may be out of date. The original file can be found at
// https://github.com/tuffy/matroska/blob/master/src/lib.rs

#![allow(dead_code)]

use std::collections::BTreeMap;
use tokio::io::AsyncRead;
use super::{ebml::{Element, ElementType, MResult}, ids};

#[derive(Debug)]
struct Seektable {
    seek: BTreeMap<u32, u64>,
}

impl Seektable {
    fn new() -> Seektable {
        Seektable {
            seek: BTreeMap::new(),
        }
    }

    #[inline]
    fn get(&self, id: u32) -> Option<u64> {
        self.seek.get(&id).cloned()
    }

    async fn parse<R: AsyncRead + Send + Sync + Unpin>(r: &mut R, size: u64) -> MResult<Seektable> {
        let mut seektable = Seektable::new();
        for e in Element::parse_master(r, size).await? {
            if let Element {
                id: ids::SEEK,
                val: ElementType::Master(sub_elements),
                ..
            } = e
            {
                let seek = Seek::build(sub_elements);
                seektable.seek.insert(seek.id(), seek.position);
            }
        }
        Ok(seektable)
    }
}


#[derive(Debug)]
struct Seek {
    id: Vec<u8>,
    position: u64,
}

impl Seek {
    fn new() -> Seek {
        Seek {
            id: Vec::new(),
            position: 0,
        }
    }

    fn id(&self) -> u32 {
        self.id.iter().fold(0, |acc, i| (acc << 8) | u32::from(*i))
    }

    fn build(elements: Vec<Element>) -> Seek {
        let mut seek = Seek::new();
        for e in elements {
            match e {
                Element {
                    id: ids::SEEKID,
                    val: ElementType::Binary(id),
                    ..
                } => {
                    seek.id = id;
                }
                Element {
                    id: ids::SEEKPOSITION,
                    val: ElementType::UInt(position),
                    ..
                } => {
                    seek.position = position;
                }
                _ => {}
            }
        }
        seek
    }
}