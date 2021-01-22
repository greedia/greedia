use std::{
    cmp::min, collections::btree_map::Entry,
    collections::BTreeMap, fmt::Debug,
};

// test

#[derive(Debug, Eq, PartialEq)]
pub enum Scan<T> {
    /// Range that contains data.
    Data {
        // Where the data starts within the byte range.
        start_offset: u64,
        size: u64,
        data: T,
    },
    /// Range between two data ranges, or the beginning and a data range.
    Gap { start_offset: u64, size: u64 },
    /// End of file.
    FinalRange { start_offset: u64, size: u64 },
    Empty,
}

impl<T> Scan<T> {
    pub fn data(start_offset: u64, size: u64, data: T) -> Scan<T> {
        Scan::Data {
            start_offset,
            size,
            data,
        }
    }

    pub fn gap(start_offset: u64, size: u64) -> Scan<T> {
        Scan::Gap { start_offset, size }
    }

    pub fn final_range(start_offset: u64, size: u64) -> Scan<T> {
        Scan::FinalRange { start_offset, size }
    }

    pub fn empty() -> Scan<T> {
        Scan::Empty
    }

    pub fn is_data(&self) -> bool {
        if let Scan::Data { start_offset: _, size: _, data: _ } = self {
            true
        } else {
            false
        }
    }

    pub fn is_gap(&self) -> bool {
        if let Scan::Gap { start_offset: _, size: _} = self {
            true
        } else {
            false
        }
    }
}

/// Hold references to structures, given ranges.
#[derive(Debug)]
pub struct ByteRanger<T> {
    byte_ranges: BTreeMap<u64, (u64, T)>,
}

impl<T> ByteRanger<T> {
    pub fn new() -> ByteRanger<T> {
        ByteRanger {
            byte_ranges: BTreeMap::new(),
        }
    }

    /// Add range to ByteRanger, ignoring any possible overlapping ranges.
    fn add_range_unchecked(&mut self, offset: u64, size: u64, data: T) {
        self.byte_ranges.insert(offset, (size, data));
    }

    /// Add range to ByteRanger, ensuring no overlapping ranges.
    pub fn add_range(&mut self, offset: u64, size: u64, data: T)
    where
        T: Clone,
    {
        if self.byte_ranges.len() == 0 {
            self.add_range_unchecked(offset, size, data);
            return;
        }

        let existing_ranges = self.scan_range(offset, size);
        if existing_ranges.len() == 0 {
            self.add_range_unchecked(offset, size, data);
        } else if let &[Scan::Gap {
            start_offset: _,
            size: _,
        }] = &existing_ranges[..]
        {
            self.add_range_unchecked(offset, size, data);
        } else {
            let mut ranges_to_add = vec![];

            {
                let existing_ranges = self.scan_range(offset, size);

                let mut data_offset = offset;
                let end_offset = offset + size;

                for s in existing_ranges {
                    if data_offset >= end_offset {
                        break;
                    }
                    match s {
                        Scan::Data {
                            start_offset,
                            size,
                            data: _,
                        } => {
                            data_offset = start_offset + size;
                        }
                        Scan::Gap { start_offset, size } => {
                            let new_size = min(end_offset - data_offset, size);
                            ranges_to_add.push((start_offset, new_size, data.clone()));
                            data_offset = end_offset;
                        }
                        Scan::FinalRange { start_offset: _, size: _ } => todo!(),
                        Scan::Empty => {},
                    }
                }
                if data_offset < end_offset {
                    // We're clear for the end, add more data
                    let new_size = end_offset - data_offset;
                    ranges_to_add.push((data_offset, new_size, data.clone()));
                }
            }

            for (start_offset, new_size, data) in ranges_to_add {
                self.add_range_unchecked(start_offset, new_size, data)
            }
        }
    }

    /// Add ranges from another ByteRanger.
    pub fn add_byte_ranger(&mut self, other: ByteRanger<T>)
    where
        T: Clone,
    {
        for (offset, (size, data)) in other.byte_ranges {
            self.add_range(offset, size, data);
        }
    }

    /// Extend the size of a range, returning the old size if successful.
    /// Returns None if original offset didn't exist, or the new size is smaller than the old size.
    pub fn extend_range(&mut self, offset: u64, mut new_size: u64) -> Option<u64> {
        match self.byte_ranges.entry(offset) {
            Entry::Vacant(_) => None,
            Entry::Occupied(mut oe) => {

                let (ref mut size, _data) = oe.get_mut();
                std::mem::swap(&mut new_size, size);
                Some(new_size)
            }
        }
    }

    /// Replace the data contents of a range, returning the old data if successful.
    /// Returns None if original offset didn't exist.
    pub fn replace_data(&mut self, offset: u64, mut new_data: T) -> Option<T> {
        match self.byte_ranges.entry(offset) {
            Entry::Vacant(_) => None,
            Entry::Occupied(mut oe) => {
                let (_size, ref mut data) = oe.get_mut();
                std::mem::swap(&mut new_data, data);
                Some(new_data)
            }
        }
    }

    /// Get a reference to a range's data.
    pub fn get_data<'a>(&'a self, offset: u64) -> Option<&'a T>
    {
        self.byte_ranges.get(&offset).map(|(_size, data)| data)
    }

    /// Get a mutable reference to a range's data.
    pub fn get_data_mut<'a>(&'a mut self, offset: u64) -> Option<&'a mut T>
    {
        self.byte_ranges.get_mut(&offset).map(|(_size, data)| data)
    }

    /// Return the range or gap at offset.
    pub fn get_range_at<'a>(&'a self, offset: u64) -> Scan<&'a T> {
        let mut last_end_offset = 0;
        let mut last_start_offset = 0;
        let mut last_size = 0;
        
        if self.byte_ranges.is_empty() {
            return Scan::Empty;
        }
        // Iter btree in reverse from offset+1 to 0 and get first value, if exists
        if let Some((inner_offset, (inner_size, inner_data))) =
            self.byte_ranges.range(..offset + 1).rev().next()
        {
            if offset == *inner_offset {
                // If provided offset is spot-on
                return Scan::Data {
                    start_offset: *inner_offset,
                    size: *inner_size,
                    data: inner_data,
                };
            } else if offset > *inner_offset && offset < *inner_offset + *inner_size {
                // If provided offset is somewhere within range's offset and size
                return Scan::Data {
                    start_offset: *inner_offset,
                    size: *inner_size,
                    data: inner_data,
                };
            } else {
                // If provided offset is after scanned range
                // Record the end offset, as this will be the beginning of the gap
                last_end_offset = *inner_offset + *inner_size;
                // Record last_start_offset and last_size for FinalRange results
                last_start_offset = *inner_offset;
                last_size = *inner_size;
            }
        }

        // There is no range, or only ranges are after provided offset
        // Check for the end of the gap, if any
        if let Some((inner_offset, (_inner_size, _inner_data))) =
            self.byte_ranges.range(last_end_offset..).next()
        {
            // Return the gap between the last range and the next one
            return Scan::Gap {
                start_offset: last_end_offset,
                size: *inner_offset - last_end_offset,
            };
        } else {
            // There is no end of the gap, so we're at EOF
            return Scan::FinalRange {
                start_offset: last_start_offset,
                size: last_size,
            };
        }
    }

    /// Return ranges within offset and size.
    pub fn scan_range<'a>(&'a self, offset: u64, size: u64) -> Vec<Scan<&'a T>> {
        let mut last_end_offset = offset;
        let mut out = vec![];

        if let Some((inner_offset, (inner_size, inner_data))) =
            self.byte_ranges.range(..offset + 1).next()
        {
            if offset == *inner_offset {
                out.push(Scan::Data {
                    start_offset: *inner_offset,
                    size: *inner_size,
                    data: inner_data,
                });
                last_end_offset = *inner_offset + *inner_size;
            } else if offset > *inner_offset && offset < *inner_offset + *inner_size {
                out.push(Scan::Data {
                    start_offset: *inner_offset,
                    size: *inner_size,
                    data: inner_data,
                });
                last_end_offset = *inner_offset + *inner_size;
            }
        }

        for (inner_offset, (inner_size, inner_data)) in self.byte_ranges.range(last_end_offset..) {
            dbg!(inner_offset, last_end_offset);
            if *inner_offset + *inner_size > last_end_offset {
                if last_end_offset < offset + size && last_end_offset != *inner_offset {
                    out.push(Scan::Gap {
                        start_offset: last_end_offset,
                        size: *inner_offset - last_end_offset,
                    });
                }
                if *inner_offset >= offset + size {
                    break;
                }
                out.push(Scan::Data {
                    start_offset: *inner_offset,
                    size: *inner_size,
                    data: inner_data,
                });
                last_end_offset = *inner_offset + *inner_size;
            }
        }

        if last_end_offset < offset + size {
            out.push(Scan::Gap {
                start_offset: last_end_offset,
                size: offset + size - last_end_offset,
            });
        }

        // TODO: handle returning Eof

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    // TODO: fix scan_range tests
    // #[rstest(input, ranges, expected,
    //     case(
    //         (50, 200),
    //         &[(0, 100), (105, 100)],
    //         &[
    //             Scan::data(0, 100, &()),
    //             Scan::gap(100, 5),
    //             Scan::data(105, 100, &())
    //         ]
    //     ),
    //     case(
    //         (0, 200),
    //         &[(0, 100), (105, 100)],
    //         &[
    //             Scan::data(0, 100, &()),
    //             Scan::gap(100, 5),
    //             Scan::data(105, 100, &())
    //         ]
    //     ),
    //     case(
    //         (50, 200),
    //         &[],
    //         &[]
    //     ),
    //     case(
    //         (0, 100),
    //         &[(0, 100), (105, 100)],
    //         &[
    //             Scan::data(0, 100, &()),
    //         ]
    //     ),
    //     case(
    //         (0, 105),
    //         &[(0, 100), (105, 100)],
    //         &[
    //             Scan::data(0, 100, &()),
    //             Scan::gap(100, 5),
    //         ]
    //     ),
    //     case(
    //         (0, 106),
    //         &[(0, 100), (105, 100)],
    //         &[
    //             Scan::data(0, 100, &()),
    //             Scan::gap(100, 5),
    //             Scan::data(105, 100, &())
    //         ]
    //     ),
    //     case(
    //         (100, 5),
    //         &[(0, 100), (105, 100)],
    //         &[
    //             Scan::gap(100, 5),
    //         ]
    //     ),
    //     case(
    //         (0, 200),
    //         &[(0, 100), (100, 100)],
    //         &[
    //             Scan::data(0, 100, &()),
    //             Scan::data(100, 100, &()),
    //         ]
    //     ),
    //     case(
    //         (100, 5),
    //         &[(0, 100), (100, 100)],
    //         &[
    //             Scan::data(100, 100, &()),
    //         ]
    //     ),
    // )]
    // fn test_scans(input: (u64, u64), ranges: &[(u64, u64)], expected: &[Scan<&()>]) {
    //     let (input_offset, input_size) = input;
    //     let mut byte_ranger = ByteRanger::new();
    //     for (offset, size) in ranges {
    //         byte_ranger.add_range_unchecked(*offset, *size, ());
    //     }

    //     let scanned_range = byte_ranger.scan_range(input_offset, input_size);
    //     assert_eq!(&scanned_range[..], expected);
    // }

    #[rstest(input, ranges, expected,
        case(
            50,
            &[(0, 100), (105, 100)],
            Scan::data(0, 100, &()),
        ),
        case(
            50,
            &[],
            Scan::empty(),
        ),
        case(
            500,
            &[(0, 100)],
            Scan::final_range(0, 100),
        ),
        case(
            50,
            &[(100, 100)],
            Scan::gap(0, 100),
        ),
        case(
            50,
            &[(0, 50), (100, 50)],
            Scan::gap(50, 50),
        ),
        case(
            102,
            &[(0, 100), (105, 100)],
            Scan::gap(100, 5),
        ),
    )]
    fn test_get_range_at(input: u64, ranges: &[(u64, u64)], expected: Scan<&()>) {
        let mut byte_ranger = ByteRanger::new();
        for (offset, size) in ranges {
            byte_ranger.add_range_unchecked(*offset, *size, ());
        }

        let get_range = byte_ranger.get_range_at(input);
        assert_eq!(get_range, expected);
    }

    // TODO: fix scan_range tests
    // #[test]
    // fn test_add_ranges() {
    //     let mut byte_ranger = ByteRanger::new();
    //     byte_ranger.add_range(0, 100, 1);
    //     byte_ranger.add_range(50, 100, 2);
    //     let scanned_range = byte_ranger.scan_range(0, 200);
    //     assert_eq!(
    //         &scanned_range[..],
    //         &[Scan::data(0, 100, &1), Scan::data(100, 50, &2)]
    //     );
    // }

    // TODO: fix scan_range tests
    // #[test]
    // fn test_add_byte_range() {
    //     let mut byte_ranger = ByteRanger::new();
    //     byte_ranger.add_range(0, 100, 1);
    //     byte_ranger.add_range(50, 100, 2);
    //     let mut byte_ranger_2 = ByteRanger::new();
    //     byte_ranger_2.add_range(100, 100, 3);
    //     byte_ranger_2.add_range(205, 100, 4);

    //     byte_ranger.add_byte_ranger(byte_ranger_2);
    //     let scanned_range = byte_ranger.scan_range(0, 400);
    //     assert_eq!(
    //         &scanned_range[..],
    //         &[
    //             Scan::data(0, 100, &1),
    //             Scan::data(100, 50, &2),
    //             Scan::data(150, 50, &3),
    //             Scan::gap(200, 5),
    //             Scan::data(205, 100, &4)
    //         ]
    //     );
    // }

    #[test]
    fn test_extend_range() {
        let mut byte_ranger = ByteRanger::new();
        byte_ranger.add_range(0, 49, 1);
        byte_ranger.add_range(50, 100, 2);

        byte_ranger.extend_range(50, 101);
        
        let b = byte_ranger.get_range_at(50);
        assert_eq!(b, Scan::data(50, 101, &2));
    }

    #[test]
    fn test_change_data() {
        let mut byte_ranger = ByteRanger::new();
        byte_ranger.add_range(0, 49, 1);
        byte_ranger.add_range(50, 100, 2);

        println!("{:?}", byte_ranger);
        let a = byte_ranger.get_data_mut(50).unwrap();
        *a = 5;
        
        let b = byte_ranger.get_data(50).unwrap();
        assert_eq!(b, &5);
    }
}
