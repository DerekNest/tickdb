/// In-memory trade buffer — the "hot" layer above the WAL.
///
/// # Design
///
/// Ingest appends to per-symbol `Vec<Trade>` buckets without sorting.
/// Sorting and deduplication happen only at flush time, keeping the ingest
/// hot path allocation-free beyond the initial push.
///
/// # Flush contract
///
/// `drain_sorted` returns trades per symbol, sorted by `(ts, trade_id)` and
/// deduplicated on `trade_id`.  This is the canonical ordering contract for
/// segment files.
///
/// # Size accounting
///
/// `byte_size` tracks an approximate byte footprint using the WAL wire size
/// (38 bytes/trade) so the caller can trigger a flush when the budget is
/// exceeded (§2.3: 256 MiB).
use std::collections::BTreeMap;

use format::types::{SymbolId, Trade};

/// Approximate memory cost per trade (matches WAL wire size).
const BYTES_PER_TRADE: usize = 38;

pub struct Memtable {
    /// Per-symbol unsorted trade buffers.
    buckets: BTreeMap<SymbolId, Vec<Trade>>,
    /// Running approximate byte count for flush-trigger decisions.
    byte_size: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            buckets: BTreeMap::new(),
            byte_size: 0,
        }
    }

    /// Appends a batch of trades into the per-symbol buckets.
    /// Trades may arrive in any order; sorting is deferred to flush.
    pub fn insert(&mut self, trades: &[Trade]) {
        for trade in trades {
            self.buckets
                .entry(trade.symbol)
                .or_default()
                .push(trade.clone());
            self.byte_size += BYTES_PER_TRADE;
        }
    }

    /// Approximate memory footprint in bytes.
    pub fn byte_size(&self) -> usize {
        self.byte_size
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Returns the set of symbol ids currently buffered.
    pub fn symbol_ids(&self) -> impl Iterator<Item = SymbolId> + '_ {
        self.buckets.keys().copied()
    }

    /// Drains all trades for `symbol_id`, returning them sorted by
    /// `(ts, trade_id)` with duplicates removed.
    ///
    /// Returns `None` if the symbol has no buffered trades.
    pub fn drain_sorted(&mut self, symbol_id: SymbolId) -> Option<Vec<Trade>> {
        let mut trades = self.buckets.remove(&symbol_id)?;
        self.byte_size = self
            .byte_size
            .saturating_sub(trades.len() * BYTES_PER_TRADE);

        // Sort by (ts, trade_id) — the segment ordering contract.
        trades.sort_unstable_by_key(|t| (t.ts, t.trade_id));

        // Dedup by trade_id.  After sorting, identical trade_ids are adjacent
        // (same trade_id implies same ts because source systems assign ids
        // monotonically within a symbol).  We keep the first occurrence.
        trades.dedup_by_key(|t| t.trade_id);

        Some(trades)
    }

    /// Drains ALL symbols, each sorted and deduplicated.
    /// Returns a vec of `(symbol_id, sorted_trades)` in ascending symbol_id order.
    pub fn drain_all_sorted(&mut self) -> Vec<(SymbolId, Vec<Trade>)> {
        let symbols: Vec<SymbolId> = self.buckets.keys().copied().collect();
        symbols
            .into_iter()
            .filter_map(|sym| self.drain_sorted(sym).map(|t| (sym, t)))
            .collect()
    }

    /// Returns a read-only iterator over all trades for `symbol_id`.
    /// Used by the engine's scan path to avoid cloning the bucket for sorting.
    pub fn iter_symbol(&self, symbol_id: SymbolId) -> impl Iterator<Item = &Trade> {
        self.buckets
            .get(&symbol_id)
            .map(|v| v.iter())
            .into_iter()
            .flatten()
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use format::dz::Decimal64;
    use format::types::Side;

    fn make_trade(symbol: SymbolId, ts: i64, trade_id: u64) -> Trade {
        Trade {
            symbol,
            ts,
            price: Decimal64 {
                mantissa: 100,
                exponent: -2,
            },
            size: 1,
            side: Side::Buy,
            trade_id,
        }
    }

    #[test]
    fn empty_memtable() {
        let mut m = Memtable::new();
        assert!(m.is_empty());
        assert_eq!(m.byte_size(), 0);
        assert!(m.drain_sorted(1).is_none());
    }

    #[test]
    fn insert_and_drain_sorted() {
        let mut m = Memtable::new();
        // Insert out of order.
        m.insert(&[
            make_trade(1, 300, 3),
            make_trade(1, 100, 1),
            make_trade(1, 200, 2),
        ]);
        let trades = m.drain_sorted(1).unwrap();
        assert_eq!(trades.len(), 3);
        assert_eq!(trades[0].ts, 100);
        assert_eq!(trades[1].ts, 200);
        assert_eq!(trades[2].ts, 300);
        assert!(m.is_empty());
    }

    #[test]
    fn dedup_by_trade_id() {
        let mut m = Memtable::new();
        // Same trade_id inserted twice (re-ingest scenario).
        m.insert(&[make_trade(1, 100, 42), make_trade(1, 100, 42)]);
        let trades = m.drain_sorted(1).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].trade_id, 42);
    }

    #[test]
    fn multi_symbol_isolation() {
        let mut m = Memtable::new();
        m.insert(&[
            make_trade(1, 100, 1),
            make_trade(2, 200, 2),
            make_trade(1, 50, 3),
        ]);

        let s1 = m.drain_sorted(1).unwrap();
        assert_eq!(s1.len(), 2);
        assert_eq!(s1[0].ts, 50);

        let s2 = m.drain_sorted(2).unwrap();
        assert_eq!(s2.len(), 1);
        assert_eq!(s2[0].symbol, 2);
    }

    #[test]
    fn byte_size_tracking() {
        let mut m = Memtable::new();
        m.insert(&[make_trade(1, 0, 0), make_trade(1, 1, 1)]);
        assert_eq!(m.byte_size(), 2 * BYTES_PER_TRADE);
        m.drain_sorted(1);
        assert_eq!(m.byte_size(), 0);
    }

    #[test]
    fn ts_tiebreak_by_trade_id() {
        let mut m = Memtable::new();
        // Same ts, different trade_ids → ordered by trade_id.
        m.insert(&[
            make_trade(1, 100, 5),
            make_trade(1, 100, 2),
            make_trade(1, 100, 9),
        ]);
        let trades = m.drain_sorted(1).unwrap();
        assert_eq!(trades[0].trade_id, 2);
        assert_eq!(trades[1].trade_id, 5);
        assert_eq!(trades[2].trade_id, 9);
    }
}
