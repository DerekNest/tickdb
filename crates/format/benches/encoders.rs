use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use format::dz::Decimal64;
use format::{dod, dz, rle, size_col};

const N: usize = 65_536; // target block size

fn bench_dod(c: &mut Criterion) {
    let ts: Vec<i64> = (0..N as i64)
        .map(|i| 1_700_000_000_000_000_000i64 + i * 100_000)
        .collect();

    let mut group = c.benchmark_group("dod");
    group.throughput(Throughput::Elements(N as u64));

    group.bench_function("encode", |b| {
        let mut buf = Vec::with_capacity(N * 10);
        b.iter(|| {
            buf.clear();
            dod::encode(black_box(&ts), &mut buf);
        });
    });

    let mut buf = Vec::new();
    dod::encode(&ts, &mut buf);
    group.bench_function("decode", |b| {
        b.iter(|| dod::decode(black_box(&buf), N).unwrap());
    });

    group.finish();
}

fn bench_dz(c: &mut Criterion) {
    let prices: Vec<Decimal64> = (0..N as i64)
        .map(|i| Decimal64 {
            mantissa: 5_000_000 + (i % 100),
            exponent: -2,
        })
        .collect();

    let mut group = c.benchmark_group("dz");
    group.throughput(Throughput::Elements(N as u64));

    group.bench_function("encode", |b| {
        let mut buf = Vec::with_capacity(N * 10);
        b.iter(|| {
            buf.clear();
            dz::encode(black_box(&prices), &mut buf);
        });
    });

    let mut buf = Vec::new();
    dz::encode(&prices, &mut buf);
    group.bench_function("decode", |b| {
        b.iter(|| dz::decode(black_box(&buf), N).unwrap());
    });

    group.finish();
}

fn bench_rle(c: &mut Criterion) {
    // Realistic: long buy runs with occasional sells.
    let sides: Vec<u8> = (0..N).map(|i| if i % 47 == 0 { 1 } else { 0 }).collect();

    let mut group = c.benchmark_group("rle");
    group.throughput(Throughput::Elements(N as u64));

    group.bench_function("encode", |b| {
        let mut buf = Vec::with_capacity(N);
        b.iter(|| {
            buf.clear();
            rle::encode(black_box(&sides), &mut buf);
        });
    });

    let mut buf = Vec::new();
    rle::encode(&sides, &mut buf);
    group.bench_function("decode", |b| {
        b.iter(|| rle::decode(black_box(&buf), N).unwrap());
    });

    group.finish();
}

fn bench_size_col(c: &mut Criterion) {
    let sizes: Vec<u64> = (0..N as u64).map(|i| 1 + (i % 1000)).collect();

    let mut group = c.benchmark_group("size_col");
    group.throughput(Throughput::Elements(N as u64));

    group.bench_function("encode", |b| {
        let mut buf = Vec::with_capacity(N * 3);
        b.iter(|| {
            buf.clear();
            size_col::encode(black_box(&sizes), &mut buf);
        });
    });

    let mut buf = Vec::new();
    size_col::encode(&sizes, &mut buf);
    group.bench_function("decode", |b| {
        b.iter(|| size_col::decode(black_box(&buf), N).unwrap());
    });

    group.finish();
}

criterion_group!(benches, bench_dod, bench_dz, bench_rle, bench_size_col);
criterion_main!(benches);
