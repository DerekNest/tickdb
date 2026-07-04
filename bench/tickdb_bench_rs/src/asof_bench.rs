use std::time::Instant;
use query::asof::{asof_join, FlatTrade};

fn main() {
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(2_000_000);

    let mut rng_state: u64 = 0xA5A5A5A5A5A5A5A5;
    let mut next = || { rng_state ^= rng_state << 13; rng_state ^= rng_state >> 7; rng_state ^= rng_state << 17; rng_state };

    let base = 1_700_000_000_000_000_000i64;
    let mut ts_a = base;
    let left: Vec<FlatTrade> = (0..n).map(|i| {
        ts_a += 900_000_000 + (next() % 200_000_000) as i64;
        FlatTrade { ts: ts_a, price: 50000.0, size: 1, trade_id: i as u64 }
    }).collect();

    let mut ts_b = base;
    let right: Vec<FlatTrade> = (0..n).map(|i| {
        ts_b += 900_000_000 + (next() % 200_000_000) as i64;
        FlatTrade { ts: ts_b, price: 3000.0, size: 1, trade_id: (n + i) as u64 }
    }).collect();

    let t0 = Instant::now();
    let result = asof_join(&left, &right, 500_000_000);
    let elapsed = t0.elapsed();

    println!("# tickdb as-of join benchmark");
    println!("left_rows: {n}");
    println!("right_rows: {n}");
    println!("elapsed: {:.4}s", elapsed.as_secs_f64());
    println!("left_rows_per_sec: {:.0}", n as f64 / elapsed.as_secs_f64());
    println!("matched: {}", result.iter().filter(|r| r.right_ts.is_some()).count());
}
