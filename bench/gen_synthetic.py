#!/usr/bin/env python3
"""
Realistic Binance aggTrades synthetic generator.
Uses smaller price steps and realistic quantity distribution.
"""
import sys, random, math

def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 2_000_000
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else 42
    rng = random.Random(seed)

    print("agg_trade_id,price,qty,first_trade_id,last_trade_id,transact_time,is_buyer_maker")

    ts_ms = 1_705_276_800_000
    price = 43251.00
    agg_id = 1

    for i in range(n):
        # ~1ms inter-arrival, tight jitter (realistic)
        ts_ms += rng.randint(950, 1050)

        # Micro price walk: mostly ±1 tick (0.01), occasionally ±2
        tick = rng.choices([-2,-1,-1,0,0,0,1,1,2], k=1)[0]
        price += tick * 0.01
        price = max(42000.0, min(44000.0, price))
        price_str = f"{price:.2f}"

        # Qty: 80% small (0.001–0.1), 20% medium (0.1–2.0)
        if rng.random() < 0.8:
            qty = rng.uniform(0.001, 0.1)
        else:
            qty = rng.uniform(0.1, 2.0)
        qty_str = f"{qty:.3f}"

        # Side: slight buy skew
        is_buyer_maker = "true" if rng.random() < 0.45 else "false"

        print(f"{agg_id},{price_str},{qty_str},{agg_id},{agg_id},{ts_ms},{is_buyer_maker}")
        agg_id += 1

if __name__ == "__main__":
    main()
