//! As-of join operator (§3.2 item 4).
//!
//! For each row in `left` (symbol A), attaches the most recent row from
//! `right` (symbol B) satisfying:
//!   `right.ts ≤ left.ts`  AND  `left.ts - right.ts ≤ tolerance_ns`
//!
//! Both streams must be ts-sorted ascending.  The algorithm is a two-pointer
//! merge: O(|left| + |right|) time, O(1) extra space beyond the output.
//!
//! # Pointer invariant
//!
//! Let `rp` be the right pointer.  We maintain:
//!
//!   **Invariant:** `right[rp].ts` is the largest timestamp in `right` that is
//!   ≤ the current `left[lp].ts`.
//!
//! Proof of maintenance:
//! - Before each left row, we advance `rp` as far as possible while
//!   `right[rp + 1].ts ≤ left[lp].ts`.  After this step, `rp` points to
//!   the most recent right row whose ts ≤ left ts — the invariant holds.
//! - We then check the tolerance: if `left[lp].ts - right[rp].ts > tolerance_ns`,
//!   no match is emitted (the right row is too old).
//! - `rp` never moves backward (left is also advancing), so the amortised
//!   cost is O(|left| + |right|).
//!
//! # Input contract
//!
//! Both `left_trades` and `right_trades` must be sorted by `(ts, trade_id)`.
//! This is guaranteed when they come from `scan::collect_all`.

/// One row of the as-of join output.
#[derive(Debug, Clone, PartialEq)]
pub struct AsOfRow {
    /// Left trade's timestamp (ns).
    pub left_ts: i64,
    /// Left trade's price (as f64 for simplicity at the API layer).
    pub left_price: f64,
    /// Left trade's size.
    pub left_size: u64,
    /// Left trade's trade_id.
    pub left_trade_id: u64,
    /// Right trade's timestamp (ns), or `None` if no match within tolerance.
    pub right_ts: Option<i64>,
    /// Right trade's price, or `None` if no match.
    pub right_price: Option<f64>,
    /// Right trade's trade_id, or `None` if no match.
    pub right_trade_id: Option<u64>,
    /// Lag: `left_ts - right_ts` in ns, or `None` if no match.
    pub lag_ns: Option<i64>,
}

/// A flat trade row used as input to `asof_join`.
#[derive(Debug, Clone)]
pub struct FlatTrade {
    pub ts: i64,
    pub price: f64,
    pub size: u64,
    pub trade_id: u64,
}

/// Performs an as-of join of `left` onto `right` with the given tolerance.
///
/// For every left row, finds the most recent right row with:
///   `right.ts ≤ left.ts` AND `left.ts − right.ts ≤ tolerance_ns`.
///
/// Returns one `AsOfRow` per left row.  Rows with no match within tolerance
/// have `right_*` fields set to `None`.
///
/// # Complexity
///
/// Time: O(|left| + |right|) — each pointer advances monotonically.
/// Space: O(|output|) = O(|left|).
pub fn asof_join(left: &[FlatTrade], right: &[FlatTrade], tolerance_ns: i64) -> Vec<AsOfRow> {
    let mut result = Vec::with_capacity(left.len());
    let mut rp: usize = 0; // right pointer

    for lrow in left {
        // Advance rp as far as right[rp+1].ts ≤ lrow.ts.
        //
        // Pointer invariant maintenance: after this loop, right[rp] is the
        // rightmost entry with ts ≤ lrow.ts (if any such entry exists).
        while rp + 1 < right.len() && right[rp + 1].ts <= lrow.ts {
            rp += 1;
        }

        // Check whether the current right candidate is within the time window.
        let matched = if rp < right.len() && right[rp].ts <= lrow.ts {
            let lag = lrow.ts - right[rp].ts;
            if lag <= tolerance_ns {
                let r = &right[rp];
                Some((r.ts, r.price, r.trade_id, lag))
            } else {
                None // right row exists but is too old
            }
        } else {
            None // no right row with ts ≤ left.ts at all
        };

        result.push(AsOfRow {
            left_ts: lrow.ts,
            left_price: lrow.price,
            left_size: lrow.size,
            left_trade_id: lrow.trade_id,
            right_ts: matched.map(|(ts, _, _, _)| ts),
            right_price: matched.map(|(_, p, _, _)| p),
            right_trade_id: matched.map(|(_, _, id, _)| id),
            lag_ns: matched.map(|(_, _, _, lag)| lag),
        });
    }

    result
}

/// Naive reference implementation of as-of join (O(|left| × |right|)).
///
/// Used in differential tests to verify the two-pointer version.
pub fn asof_join_ref(left: &[FlatTrade], right: &[FlatTrade], tolerance_ns: i64) -> Vec<AsOfRow> {
    let mut result = Vec::with_capacity(left.len());

    for lrow in left {
        // Find the most recent right row with ts ≤ lrow.ts within tolerance.
        let matched = right
            .iter()
            .filter(|r| r.ts <= lrow.ts && lrow.ts - r.ts <= tolerance_ns)
            .max_by_key(|r| r.ts);

        result.push(AsOfRow {
            left_ts: lrow.ts,
            left_price: lrow.price,
            left_size: lrow.size,
            left_trade_id: lrow.trade_id,
            right_ts: matched.map(|r| r.ts),
            right_price: matched.map(|r| r.price),
            right_trade_id: matched.map(|r| r.trade_id),
            lag_ns: matched.map(|r| lrow.ts - r.ts),
        });
    }

    result
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ft(ts: i64, price: f64, trade_id: u64) -> FlatTrade {
        FlatTrade {
            ts,
            price,
            size: 1,
            trade_id,
        }
    }

    #[test]
    fn empty_inputs() {
        assert!(asof_join(&[], &[], 1000).is_empty());
        assert!(asof_join(&[ft(100, 1.0, 1)], &[], 1000)[0]
            .right_ts
            .is_none());
    }

    #[test]
    fn exact_match() {
        let left = vec![ft(100, 50.0, 1)];
        let right = vec![ft(100, 49.0, 2)];
        let result = asof_join(&left, &right, 0);
        assert_eq!(result[0].right_ts, Some(100));
        assert_eq!(result[0].lag_ns, Some(0));
    }

    #[test]
    fn within_tolerance() {
        let left = vec![ft(105, 50.0, 1)];
        let right = vec![ft(100, 49.0, 2)];
        let result = asof_join(&left, &right, 10);
        assert_eq!(result[0].right_ts, Some(100));
        assert_eq!(result[0].lag_ns, Some(5));
    }

    #[test]
    fn outside_tolerance_no_match() {
        let left = vec![ft(200, 50.0, 1)];
        let right = vec![ft(100, 49.0, 2)];
        let result = asof_join(&left, &right, 50);
        assert!(result[0].right_ts.is_none());
    }

    #[test]
    fn attaches_most_recent() {
        let left = vec![ft(300, 50.0, 1)];
        let right = vec![ft(100, 49.0, 2), ft(250, 49.5, 3), ft(200, 48.0, 4)];
        // right sorted: 100, 200, 250
        let mut right_sorted = right.clone();
        right_sorted.sort_by_key(|r| r.ts);
        let result = asof_join(&left, &right_sorted, 1000);
        // Most recent right with ts ≤ 300 is ts=250 (trade_id=3).
        assert_eq!(result[0].right_trade_id, Some(3));
    }

    #[test]
    fn pointer_never_moves_backward() {
        // Multiple left rows advancing through right — verify O(n+m) behaviour
        // by checking results equal the reference.
        let left: Vec<FlatTrade> = (0..20).map(|i| ft(i * 10, 50.0, i as u64)).collect();
        let right: Vec<FlatTrade> = (0..20).map(|i| ft(i * 7, 49.0, 100 + i as u64)).collect();
        let fast = asof_join(&left, &right, 100);
        let slow = asof_join_ref(&left, &right, 100);
        for (f, s) in fast.iter().zip(slow.iter()) {
            assert_eq!(f.right_ts, s.right_ts, "mismatch at left_ts={}", f.left_ts);
            assert_eq!(f.lag_ns, s.lag_ns);
        }
    }

    #[test]
    fn no_right_row_before_first_left() {
        let left = vec![ft(50, 50.0, 1)];
        let right = vec![ft(100, 49.0, 2), ft(200, 49.0, 3)];
        let result = asof_join(&left, &right, i64::MAX);
        // No right row with ts ≤ 50.
        assert!(result[0].right_ts.is_none());
    }
}
