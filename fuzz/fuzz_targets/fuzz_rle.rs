#![no_main]
use libfuzzer_sys::fuzz_target;
use format::rle;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    let n_rows = u16::from_le_bytes([data[0], data[1]]) as usize % 4096;
    let src = &data[2..];
    let _ = rle::decode(src, n_rows);
});
