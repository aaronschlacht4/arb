"""
Clean the RAW Kalshi historical trades into a trade-level CSV.

INPUT  (raw, never modified): data/raw/kalshi_trades_<ticker>.jsonl
OUTPUT (derived, overwritable): data/clean/kalshi_trades_<ticker>.csv

Emits the SHARED cross-venue schema (see notes/data_dictionary_polymarket.md) so
the Kalshi and Polymarket clean tapes are column-for-column identical and can be
stacked / compared directly for the arbitrage step. Venue-specific columns that
don't apply to Kalshi (tx_hash/maker/taker) are written blank.

FIELD SEMANTICS (identical meaning across venues):
  outcome   YES/NO  — the contract that traded. YES = "the event happens"
                      (here: a Democrat / Mamdani wins).
  side      BUY/SELL — TAKER (aggressor) perspective. Kalshi tags the book side
                      the taker hit: `ask` => taker bought, `bid` => taker sold.
  price     0-1     — price of the traded `outcome` contract (yes_price if the
                      trade was on YES, else no_price).
  quantity         — `count_fp`, number of contracts (each settles $0/$1).
  notional_usd     — price * quantity.
  yes_price 0-1    — canonical implied P(event); for Kalshi this is always
                      yes_price_dollars. This is the column to compare across
                      venues (= implied probability the event happens).

Run:
    python src/clean_kalshi_trades.py KXMAYORNYCPARTY-25-D
"""

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"

DEFAULT_TICKER = "KXMAYORNYCPARTY-25-D"

# Shared cross-venue schema (must stay identical in clean_polymarket_*.py).
FIELDS = [
    "venue", "timestamp", "unix_ts", "trade_id", "outcome", "side",
    "price", "quantity", "notional_usd", "yes_price",
    "tx_hash", "maker", "taker", "is_block_trade",
]


def parse_ts(s: str) -> datetime:
    """Kalshi created_time is RFC3339 with a trailing 'Z'; parse to aware UTC."""
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def clean_row(t: dict) -> dict:
    dt = parse_ts(t["created_time"])
    yes = float(t["yes_price_dollars"])
    no = float(t["no_price_dollars"])
    qty = float(t["count_fp"])
    qty = int(qty) if qty.is_integer() else qty
    outcome = t["taker_outcome_side"].upper()            # YES / NO
    price = yes if outcome == "YES" else no              # price of the traded contract
    side = "BUY" if t["taker_book_side"] == "ask" else "SELL"  # taker aggressor
    return {
        "venue": "kalshi",
        "timestamp": dt.isoformat(),
        "unix_ts": int(dt.timestamp()),
        "trade_id": t["trade_id"],
        "outcome": outcome,
        "side": side,
        "price": round(price, 6),
        "quantity": qty,
        "notional_usd": round(price * qty, 6),
        "yes_price": round(yes, 6),
        "tx_hash": "",       # n/a for Kalshi (centralized venue)
        "maker": "",
        "taker": "",
        "is_block_trade": t["is_block_trade"],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Clean raw Kalshi trades into a trade-level CSV.")
    ap.add_argument("ticker", nargs="?", default=DEFAULT_TICKER)
    args = ap.parse_args()

    raw_path = RAW_DIR / f"kalshi_trades_{args.ticker}.jsonl"
    clean_path = CLEAN_DIR / f"kalshi_trades_{args.ticker}.csv"
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    if not raw_path.exists():
        raise SystemExit(f"Raw file not found: {raw_path}")

    rows = [clean_row(json.loads(line)) for line in raw_path.open()]
    rows.sort(key=lambda r: (r["unix_ts"], r["trade_id"]))

    with clean_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    # --- summary / sanity ---
    from collections import Counter
    by_outcome = Counter(r["outcome"] for r in rows)
    contracts = sum(r["quantity"] for r in rows)
    notional = sum(r["notional_usd"] for r in rows)
    print(f"trades written : {len(rows):,}  -> {clean_path}")
    print(f"  by outcome   : {dict(by_outcome)}")
    if rows:
        print(f"  date range   : {rows[0]['timestamp'][:10]} .. {rows[-1]['timestamp'][:10]}")
        print(f"  yes_price rng: {min(r['yes_price'] for r in rows)} .. {max(r['yes_price'] for r in rows)}")
        print(f"  contracts    : {contracts:,.0f}")
        print(f"  notional     : ${notional:,.0f}")


if __name__ == "__main__":
    main()
