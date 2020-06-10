use super::{HardCacheDownloader, smart_cacher::{
    FileSpec, ScErr::*, ScOk::*, ScResult, SmartCacher,
    SmartCacherSpec,
}};
use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder};
use crate::config::SmartCacherConfig;

// Related resources:
// - https://www.codeproject.com/Articles/8295/MPEG-Audio-Frame-Header
// - http://fileformats.archiveteam.org/wiki/ID3
// - https://github.com/Herschel/puremp3 was used as a reference for much of the MP3 header handling.

static SPEC: SmartCacherSpec = SmartCacherSpec {
    name: "mp3_testing",
    version: 0,
    exts: &["mp3"],
    header_bytes: 65536,
};

/// Smart cacher for MP3 files.
pub struct ScMp3;

#[async_trait]
impl SmartCacher for ScMp3 {
    fn spec(&self) -> &'static SmartCacherSpec {
        &SPEC
    }

    async fn cache(
        &self,
        header_data: &[u8],
        config: &SmartCacherConfig,
        file_spec: &FileSpec,
        action: &mut HardCacheDownloader,
    ) -> ScResult {
        // First, look for a starting ID3v2 tag and skip over it if necessary.
        let header_scan_offset =
            if let Some(id3_len) = read_id3_len(header_data.get(..10).ok_or(Cancel)?) {
                id3_len as usize
            } else {
                0
            };

        // Next, try to scan for the MP3 header.

        // Get a decently-sized buffer for scanning.
        let header_scan_buffer = if header_scan_offset == 0 {
            header_data
        } else {
            action
                .read_data_bridged(header_scan_offset, 65536, None)
                .await
        };

        // Now, look for the actual MP3 header.
        let (mp3_header_offset, mp3_header_data) =
            find_mp3_header(header_scan_buffer).ok_or(Cancel)?;
        let mp3_header_offset = mp3_header_offset + header_scan_offset;

        // Get the bitrate out of the header.
        // TODO: handle VBR. For now this assumes CBR.
        let mp3_header = get_mp3_header(mp3_header_data).ok_or(Cancel)?;

        // Using the bitrate, estimate how many bytes are needed to download.
        let data_length = mp3_header.bitrate * config.seconds;

        // Download up to that offset.
        action.cache_data_to(mp3_header_offset + data_length as usize);

        // Cache the ID3v1 tag location, just in case it exists.
        action.cache_data(file_spec.size as usize - 128, 128);
        Ok(Finalize)
    }
}

/// Read the length of the ID3 header, to figure out where to scan
fn read_id3_len(data: &[u8]) -> Option<u32> {
    if data.get(..3)? == b"ID3" {
        let len_data = BigEndian::read_u32(data.get(6..10)?);
        // Verify that none of the MSBs are set
        if len_data & 0x80808080 != 0 {
            return None;
        }

        // Mask and shift!
        Some(
            len_data & 0x7F
                | (len_data & 0x7F00) >> 1
                | (len_data & 0x7F0000) >> 2
                | (len_data & 0x7F000000) >> 3,
        )
    } else {
        None
    }
}

fn find_mp3_header<'a>(data: &'a [u8]) -> Option<(usize, &'a [u8])> {
    let header_offset = data.iter().position(|x| *x == 0xff)?;
    if data.get(header_offset + 1)? & 0xE0 != 0xE0 {
        Some((header_offset, data.get(header_offset..header_offset + 8)?))
    } else {
        None
    }
}

#[derive(PartialEq, Copy, Clone)]
enum MpegVersion {
    Mpeg1,
    Mpeg2,
    Mpeg2_5,
}

struct Mp3Header {
    bitrate: u64,
}

fn get_mp3_header(data: &[u8]) -> Option<Mp3Header> {
    // Byte containing version and layer information.
    let vl_byte = data.get(1)?;

    // Cancel caching if this file is MP1 or MP2.
    if vl_byte & 0b110 != 0b010 {
        None?
    }

    let version = match vl_byte & 0b0001_1000 {
        0b00_000 => MpegVersion::Mpeg2_5,
        0b10_000 => MpegVersion::Mpeg2,
        0b11_000 => MpegVersion::Mpeg1,
        _ => None?,
    };

    let is_version2 = version == MpegVersion::Mpeg2 || version == MpegVersion::Mpeg2_5;

    // Byte containing bitrate and sample rate information.
    let br_byte = data.get(2)?;

    let bitrate = match (br_byte & 0b1111_0000, is_version2) {
        (0b0001_0000, false) => 32_000,
        (0b0010_0000, false) => 40_000,
        (0b0011_0000, false) => 48_000,
        (0b0100_0000, false) => 56_000,
        (0b0101_0000, false) => 64_000,
        (0b0110_0000, false) => 80_000,
        (0b0111_0000, false) => 96_000,
        (0b1000_0000, false) => 112_000,
        (0b1001_0000, false) => 128_000,
        (0b1010_0000, false) => 160_000,
        (0b1011_0000, false) => 192_000,
        (0b1100_0000, false) => 224_000,
        (0b1101_0000, false) => 256_000,
        (0b1110_0000, false) => 320_000,

        (0b0001_0000, true) => 8_000,
        (0b0010_0000, true) => 16_000,
        (0b0011_0000, true) => 24_000,
        (0b0100_0000, true) => 32_000,
        (0b0101_0000, true) => 40_000,
        (0b0110_0000, true) => 48_000,
        (0b0111_0000, true) => 56_000,
        (0b1000_0000, true) => 64_000,
        (0b1001_0000, true) => 80_000,
        (0b1010_0000, true) => 96_000,
        (0b1011_0000, true) => 112_000,
        (0b1100_0000, true) => 128_000,
        (0b1101_0000, true) => 144_000,
        (0b1110_0000, true) => 160_000,

        _ => None?,
    };

    Some(Mp3Header { bitrate })
}
