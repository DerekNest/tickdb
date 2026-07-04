/// Core domain types shared across all tickdb crates.
///
/// These match the fixed schema v1 exactly as specified.  No dynamic schema,
/// no optional columns — the schema is locked for the lifetime of the project.
pub use crate::dz::Decimal64;

/// Dictionary-encoded symbol identifier.  The mapping `u32 → utf8` lives in
/// the segment footer; the node-global symbol table maps names to ids on ingest.
pub type SymbolId = u32;

/// Aggressor side of a trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum Side {
    /// Buy-aggressor: the buyer was the active (crossing) party.
    Buy = 0,
    /// Sell-aggressor: the seller was the active party.
    Sell = 1,
    /// Unknown: exchange did not report aggressor side.
    Unknown = 2,
}

impl Side {
    /// Converts a raw `u8` to `Side`.  Values outside `{0, 1, 2}` become
    /// `Side::Unknown` rather than panicking — important for fuzz safety.
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Buy,
            1 => Self::Sell,
            _ => Self::Unknown,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// A single trade record — the canonical ingest and WAL representation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Trade {
    /// Dictionary-encoded symbol id.
    pub symbol: SymbolId,
    /// Nanoseconds since Unix epoch, UTC.
    pub ts: i64,
    /// Fixed-point price.  `actual = mantissa × 10^exponent`.
    pub price: Decimal64,
    /// Quantity in minor units (shares / satoshis).
    pub size: u64,
    /// Aggressor side.
    pub side: Side,
    /// Source-assigned trade identifier; used for deduplication on re-ingest.
    pub trade_id: u64,
}
