//! Timecode_rs is a tool that allows you to test and debug offsets. It can create a file that encodes offsets
//! within the data itself, which allows you to determine the offset of a piece of data with 11 bytes.
//!
//! It is named after timecode vinyl records, which digitally encode offsets to map a position to an audio file.
//!
//! The format of the file is pretty simple: a sequence of 32-bit integers in network byte order:
//! [00 00 00 00] [00 00 00 01] [00 00 00 02] etc.
//!
//! In order to discover our offset, we need to read 11 bytes, which allows us to read 2 u32s with a 3-byte
//! sliding window.
//! ```text
//! 00 01 02 03 04 05 06 07 08 09 0A
//! [         ] [         ] on the first round
//!    [         ] [         ] on the second round
//!       [         ] [         ] on the third round
//!          [         ] [         ] on the fourth round
//! ```
//!
//! To discover the offset, we need to find the first whole u32. We make three attempts with the sliding window
//! to find 8 bytes that make up two u32s in a row, where second == first + 1.
//!
//! Once we discover this first u32, we multiply it by 4 and add the sliding window (0-3), which gives us our offset.

use std::{cmp::{max, min}, convert::TryInto};
use std::io::Read;

/// Because we encode with u32s, the maximum offset we can discover is u32::MAX*4 - 11.
pub const MAX_SIZE: u64 = u32::MAX as u64 * 4 - 11; // 17179869169

/// Returns a writer starting at `offset`, which will write out timecode data that we can save elsewhere.
/// Will continually write data until it reaches `MAX_SIZE`.
///
/// Panics if offset given is greater than MAX_SIZE.
pub fn get_timecode(offset: u64) -> TimecodeReader {
    if offset > MAX_SIZE {
        panic!("offset must not exceed MAX_SIZE of {}", MAX_SIZE);
    }
    println!("TIMECODE, start reading at offset {}", offset);
    TimecodeReader {
        current_offset: offset,
    }
}

/// Timecode data reader, which can then be written to a file or elsewhere via the `Read` trait.
pub struct TimecodeReader {
    current_offset: u64,
}

impl Read for TimecodeReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Short-circuit for when we're at the end of the file.
        if self.current_offset > MAX_SIZE + 11 {
            return Ok(0);
        }

        let mut buf_offset = 0;
        let mut u32_offset = (self.current_offset / 4) as u32;
        let window_offset = self.current_offset % 4;

        // If we're in between two u32s.
        if window_offset != 0 {
            // Add the end of a partial u32 first.
            let cur_u32 = u32_offset.to_be_bytes();
            let w_len = (4 - window_offset) as usize;
            buf[..w_len].copy_from_slice(&cur_u32[(4 - w_len)..]);
            buf_offset += w_len;
            u32_offset += 1;
        }

        // Keep adding u32s until we run out of space in the buffer.
        loop {
            let cur_u32 = u32_offset.to_be_bytes();
            if buf.len() - buf_offset >= 4 {
                // Add a whole u32
                buf[buf_offset..buf_offset + 4].copy_from_slice(&cur_u32);

                buf_offset += 4;
                if u32_offset == u32::MAX {
                    break;
                } else {
                    u32_offset += 1;
                }
            } else {
                // Add the beginning of a partial u32
                let w_len = buf.len() - buf_offset;
                buf[buf_offset..].copy_from_slice(&cur_u32[..w_len]);
                buf_offset += w_len;
                break;
            }
        }

        self.current_offset += buf_offset as u64;
        Ok(buf_offset)
    }
}

/// Reads encoded offset from a slice of data that is at least 11 bytes.
///
/// Panics if data length is less than 11 bytes.
pub fn read_offset(data: &[u8]) -> Option<u64> {
    if data.len() < 11 {
        panic!(
            "read_offset data length must not be less than 11, was given {}",
            data.len()
        );
    }

    // Attempt to find two sequential u32s three times, with a sliding window.
    for i in 0..4 {
        let first = u32::from_be_bytes(data[i..i + 4].try_into().unwrap()) as u64;
        let second = u32::from_be_bytes(data[i + 4..i + 8].try_into().unwrap()) as u64;
        if second == first + 1 {
            return Some(first * 4 - i as u64);
        }
    }

    None
}

/// Reads encoded offset from a slice of data that is at least 11 bytes.
/// Also validate the rest of the data, to ensure it is properly formed.
///
/// Panics if data length is less than 11 bytes.
pub fn validate_offset(offset: u64, buf: &[u8]) -> bool {
    if buf.len() < 11 {
        panic!(
            "read_offset data length must not be less than 11, was given {}",
            buf.len()
        );
    }

    if let Some(read_offset) = read_offset(buf) {
        println!("TIMECODE READ OFFSET: {}", read_offset);
    }

    // Use the same strategy as the reader, but instead of writing, just compare with what we expect to write.
    let mut buf_offset = 0;
    let mut u32_offset = (offset / 4) as u32;
    let window_offset = offset % 4;

    // If we're in between two u32s.
    if window_offset != 0 {
        // Check for the partial u32 first.
        let cur_u32 = u32_offset.to_be_bytes();
        let w_len = (4 - window_offset) as usize;
        if &buf[..w_len] != &cur_u32[(4 - w_len)..] {
            let cur_offset = offset + buf_offset as u64;
            println!("TIMECODE VALIDATION FAILED (pre)");
            println!(
                "Start {}, offset {}: expected {:?} but found {:?}",
                offset,
                cur_offset,
                &cur_u32[(4 - w_len)..],
                &buf[..w_len]
            );
            println!("After:  {:?}", &buf[buf_offset+(4-w_len)..min(buf_offset+(4-w_len)+12, buf.len())]);
            return false;
        }
        buf_offset += w_len;
        u32_offset += 1;
    }

    // Keep checking u32s until we run out of buffer.
    loop {
        let cur_u32 = u32_offset.to_be_bytes();
        if buf.len() - buf_offset >= 4 {
            // Compare a whole u32
            if &buf[buf_offset..buf_offset + 4] != &cur_u32 {
                let cur_offset = offset + buf_offset as u64;
                let buf_num = u32::from_be_bytes(buf[buf_offset..buf_offset + 4].try_into().unwrap());
                println!("TIMECODE VALIDATION FAILED (middle)");
                println!(
                    "Start {}, offset {}: expected {:?} ({} x4 = {}) but found {:?} ({} x4 = {})",
                    offset,
                    cur_offset,
                    &cur_u32,
                    u32_offset,
                    u32_offset*4,
                    &buf[buf_offset..buf_offset + 4],
                    buf_num,
                    buf_num*4,
                );
                if buf_offset > 12 {
                    println!("Before: {:?}", &buf[max(0, buf_offset - 12)..buf_offset]);
                }
                if buf_offset + 12 < buf.len() {
                    println!("After:  {:?}", &buf[buf_offset+4..min(buf_offset+4+12, buf.len())]);
                }
                return false;
            }

            buf_offset += 4;
            if u32_offset == u32::MAX {
                break;
            } else {
                u32_offset += 1;
            }
        } else {
            // Add the beginning of a partial u32
            let w_len = buf.len() - buf_offset;
            if &buf[buf_offset..] != &cur_u32[..w_len] {
                let cur_offset = offset + buf_offset as u64;
                println!("TIMECODE VALIDATION FAILED (post)");
                println!(
                    "Start {}, offset {}: expected {:?} but found {:?}",
                    offset,
                    cur_offset,
                    &cur_u32[..w_len],
                    &buf[buf_offset..]
                );
                println!("Before: {:?}", &buf[max(0, buf_offset-12)..buf_offset]);
                return false;
            }
            break;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use proptest::prelude::*;
    use rstest::*;

    use crate::*;

    #[rstest(input,
        case(0),
        case(1),
        case(2),
        case(3),
        case(4),
        case(MAX_SIZE),
        case(MAX_SIZE-1),
        case(MAX_SIZE-2),
        case(MAX_SIZE-3),
        case(MAX_SIZE-4),
    )]
    fn timecode_test(input: u64) {
        let mut tr = get_timecode(input);

        let mut buf = [0u8; 11];
        let read_len = tr.read(&mut buf).unwrap();

        assert_eq!(read_len, 11);

        assert_eq!(input, read_offset(&buf).unwrap());
    }

    proptest! {
        #[test]
        fn timecode_test_prop(input in 0..MAX_SIZE) {
            timecode_test(input)
        }
    }
}
