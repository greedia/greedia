// Copyright 2017-2020 Brian Langenberger
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// This file may be out of date. The original file can be found at
// https://github.com/tuffy/matroska/blob/master/src/ids.rs

#![allow(dead_code)]

pub const HEADER: u32 = 0x1A45_DFA3;
pub const SEGMENT: u32 = 0x1853_8067;
pub const SEEKHEAD: u32 = 0x114D_9B74;
pub const SEEK: u32 = 0x4DBB;
pub const SEEKID: u32 = 0x53AB;
pub const SEEKPOSITION: u32 = 0x53AC;
pub const INFO: u32 = 0x1549_A966;
pub const TITLE: u32 = 0x7BA9;
pub const MUXINGAPP: u32 = 0x4D80;
pub const WRITINGAPP: u32 = 0x5741;
pub const DATEUTC: u32 = 0x4461;
pub const TIMECODESCALE: u32 = 0x2A_D7B1;
pub const DURATION: u32 = 0x4489;
pub const TRACKS: u32 = 0x1654_AE6B;
pub const TRACKENTRY: u32 = 0xAE;
pub const TRACKNUMBER: u32 = 0xD7;
pub const TRACKUID: u32 = 0x73C5;
pub const TRACKTYPE: u32 = 0x83;
pub const FLAGENABLED: u32 = 0xB9;
pub const FLAGDEFAULT: u32 = 0x88;
pub const FLAGFORCED: u32 = 0x55AA;
pub const FLAGLACING: u32 = 0x9C;
pub const DEFAULTDURATION: u32 = 0x23_E383;
pub const NAME: u32 = 0x536E;
pub const LANGUAGE: u32 = 0x22_B59C;
pub const CODEC_ID: u32 = 0x86;
pub const CODEC_NAME: u32 = 0x25_8688;
pub const VIDEO: u32 = 0xE0;
pub const PIXELWIDTH: u32 = 0xB0;
pub const PIXELHEIGHT: u32 = 0xBA;
pub const DISPLAYWIDTH: u32 = 0x54B0;
pub const DISPLAYHEIGHT: u32 = 0x54BA;
pub const AUDIO: u32 = 0xE1;
pub const SAMPLINGFREQUENCY: u32 = 0xB5;
pub const CHANNELS: u32 = 0x9F;
pub const BITDEPTH: u32 = 0x6264;
pub const ATTACHMENTS: u32 = 0x1941_A469;
pub const ATTACHEDFILE: u32 = 0x61A7;
pub const FILEDESCRIPTION: u32 = 0x467E;
pub const FILENAME: u32 = 0x466E;
pub const FILEMIMETYPE: u32 = 0x4660;
pub const FILEDATA: u32 = 0x465C;
pub const CHAPTERS: u32 = 0x1043_A770;
pub const EDITIONENTRY: u32 = 0x45B9;
pub const EDITIONFLAGHIDDEN: u32 = 0x45BD;
pub const EDITIONFLAGDEFAULT: u32 = 0x45DB;
pub const EDITIONFLAGORDERED: u32 = 0x45DD;
pub const CHAPTERATOM: u32 = 0xB6;
pub const CHAPTERTIMESTART: u32 = 0x91;
pub const CHAPTERTIMEEND: u32 = 0x92;
pub const CHAPTERFLAGHIDDEN: u32 = 0x98;
pub const CHAPTERFLAGENABLED: u32 = 0x4598;
pub const CHAPTERDISPLAY: u32 = 0x80;
pub const CHAPSTRING: u32 = 0x85;
pub const CHAPLANGUAGE: u32 = 0x437C;