"""
Clean the RAW Kalshi historical trades into a trade-level CSV.

INPUT  (raw, never modified): data/raw/kalshi_trades_<ticker>.jsonl
OUTPUT (derived, overwritable): data/clean/kalshi_trades_<ticker>.csv

Emits the SHARED cross-venue schema (see notes/data_dictionary_polymarket.md) so
the Kalshi and Polymarket clean tapes are column-for-column identical and can be
stacked / compared directly for the arbitrage step. Venue-specific columns that
don't apply to Kalshi (tx_hash/maker/taker) are written blank.

FIELD SEMANTICS (identical meaning across venues):
  outcome   YES/NO  — the outcome the TAKER is positioned for, i.e. the contract
                      the taker ends up long. Straight from `taker_outcome_side`.
  side      BUY      — ALWAYS "BUY" on Kalshi. See the note below; this is not a
                      placeholder, it is the correct value.
  price     0-1     — price of the traded `outcome` contract (yes_price if the
                      trade was on YES, else no_price).
  quantity         — `count_fp`, number of contracts (each settles $0/$1).
  notional_usd     — price * quantity.
  yes_price 0-1    — canonical implied P(event); for Kalshi this is always
                      yes_price_dollars. This is the column to compare across
                      venues (= implied probability the event happens).

WHY `side` IS ALWAYS "BUY" (corrected 2026-07-12 — this was previously wrong):
  Kalshi's public trade feed has NO buy/sell field, and cannot have one. Per the
  API docs, `taker_outcome_side` is the outcome the taker is POSITIONED FOR:
      "buy-yes and sell-no produce 'yes'; buy-no and sell-yes produce 'no'"
  so buy-YES and sell-NO are folded into the SAME record. And `taker_book_side`
  is merely a restatement of it:
      "'bid' is equivalent to taker_outcome_side 'yes'; 'ask' is equivalent to
       taker_outcome_side 'no'"
  We confirmed this empirically: across POPVOTE-24-R/-D, PRES-2024-DJT and both
  KXMAYORNYCPARTY-25 markets, ONLY the pairs (bid,yes) and (ask,no) ever occur —
  never (ask,yes) or (bid,no). The two fields are perfectly collinear.

  The old rule `side = "BUY" if taker_book_side == "ask" else "SELL"` therefore
  did NOT recover trade direction. It silently re-encoded `outcome` (tagging every
  YES trade "SELL" and every NO trade "BUY"), which is both uninformative and
  backwards.

  The honest normalization: every Kalshi taker ACQUIRES the `outcome` side, so
  side is always BUY, and `outcome` carries the direction. This is also what makes
  the venues comparable — a Polymarket "SELL YES" is a Kalshi "BUY NO".
  (Downstream is unaffected: merge_for_arb.py derives its own side from `outcome`.)

Run (event-driven — ticker + paths from events/<slug>/event.json):
    python src/clean_kalshi_trades.py mamdani-dem-nomination
"""

import argparse
import csv
import json
from datetime import datetime, timezone

from eventlib import load_event

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
    outcome = t["taker_outcome_side"].upper()            # YES / NO the taker is long
    price = yes if outcome == "YES" else no              # price of the traded contract
    # Kalshi has no buy/sell field: the taker always ACQUIRES `outcome`. See the
    # module docstring — deriving side from taker_book_side is a known trap.
    side = "BUY"
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
    ap.add_argument("event", help="Event slug, e.g. mamdani-dem-nomination")
    args = ap.parse_args()

    ev = load_event(args.event)
    raw_path = ev.kalshi_raw_jsonl
    clean_path = ev.kalshi_clean_csv
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
