#![no_main]
use libfuzzer_sys::fuzz_target;
use format::varint;

// Fuzz the varint decoder with arbitrary byte sequences.
//
// Invariant: decode_u64 must NEVER panic on any input.  It may return Err,
// but panics or UB would indicate a memory-safety issue or logic bug.
fuzz_target!(|data: &[u8]| {
    // Single-value decode.
    let _ = varint::decode_u64(data);
    let _ = varint::decode_i64(data);

    // Slice decode with varying row counts.  Use the first byte (if any) as the
    // row count hint to exercise the allocation guard with plausible values.
    let n_rows = if data.is_empty() { 0 } else { data[0] as usize };
    let src = if data.is_empty() { data } else { &data[1..] };
    let _ = varint::decode_u64_slice(src, n_rows);
});
