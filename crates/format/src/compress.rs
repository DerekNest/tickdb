/// Outer block compression: zstd-3 (cold default) and a passthrough (no compression).
///
/// lz4 is in the allowed-deps list and will be added once the build environment
/// supports lz4_flex ≥ 0.11.  For now, the flag byte space is reserved so
/// the file format is forward-compatible.
///
/// # Block header compression flags (low 2 bits of `encoding_flags`)
///
/// ```text
/// 0b00  →  NONE  (raw domain-encoded bytes)
/// 0b01  →  LZ4   (reserved; decoder returns Error::UnknownCompression until implemented)
/// 0b10  →  ZSTD3 (zstd compression level 3)
/// ```
use crate::Error;

pub const COMPRESS_NONE: u8 = 0x00;
pub const COMPRESS_LZ4: u8 = 0x01;
pub const COMPRESS_ZSTD3: u8 = 0x02;

/// Compresses `data` with the requested method, appending to `out`.
/// Returns the flag byte that identifies the method actually used.
pub fn compress(data: &[u8], method: u8, out: &mut Vec<u8>) -> Result<u8, Error> {
    match method {
        COMPRESS_NONE => {
            out.extend_from_slice(data);
            Ok(COMPRESS_NONE)
        }
        COMPRESS_LZ4 => {
            // lz4_flex not yet available in this build environment; fall through to zstd.
            compress_zstd(data, out)?;
            Ok(COMPRESS_ZSTD3)
        }
        COMPRESS_ZSTD3 => {
            compress_zstd(data, out)?;
            Ok(COMPRESS_ZSTD3)
        }
        other => Err(Error::UnknownCompression(other)),
    }
}

/// Decompresses `data` according to `flag`, writing into `out`.
pub fn decompress(data: &[u8], flag: u8, out: &mut Vec<u8>) -> Result<(), Error> {
    match flag {
        COMPRESS_NONE => {
            out.extend_from_slice(data);
            Ok(())
        }
        COMPRESS_LZ4 => Err(Error::UnknownCompression(COMPRESS_LZ4)),
        COMPRESS_ZSTD3 => {
            let decoded = zstd::decode_all(data).map_err(|e| Error::Zstd(e.to_string()))?;
            out.extend_from_slice(&decoded);
            Ok(())
        }
        other => Err(Error::UnknownCompression(other)),
    }
}

fn compress_zstd(data: &[u8], out: &mut Vec<u8>) -> Result<(), Error> {
    let encoded = zstd::encode_all(data, 3).map_err(|e| Error::Zstd(e.to_string()))?;
    out.extend_from_slice(&encoded);
    Ok(())
}
