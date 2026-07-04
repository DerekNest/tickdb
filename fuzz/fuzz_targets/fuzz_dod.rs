#![no_main]
use libfuzzer_sys::fuzz_target;
use format::dod;

// Fuzz the delta-of-delta timestamp decoder.
//
// The decoder must never panic or OOM regardless of input.  The allocation
// guard (n_rows vs remaining bytes) prevents OOM; the varint decoder
// propagates errors rather than panicking.
fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    // Use the first two bytes as a u16 row count.  Cap at 4096 so the
    // fuzzer exercises the guard without timing out on enormous row counts.
    let n_rows = u16::from_le_bytes([data[0], data[1]]) as usize % 4096;
    let src = &data[2..];
    let _ = dod::decode(src, n_rows);
});
