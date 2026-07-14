"""
Clean the SUBGRAPH-sourced raw Polymarket fills into a trade-level CSV.

INPUT  (raw, never modified): data/raw/polymarket_subgraph_orderfilled_<conditionId>.jsonl
OUTPUT (derived, overwritable): data/clean/polymarket_subgraph_trades_<conditionId>.csv

Subgraph-method twin of `clean_polymarket_rpc.py`. Emits the shared cross-venue
schema (see notes/data_dictionary_polymarket.md) so all clean tapes are identical.

TRANSFORMATIONS (all three verified against on-chain truth 2026-07-12 — see below):

  1. AGGREGATE DROP. Every economic trade emits one row per matched maker order
     PLUS one aggregate taker-order row (taker == the Exchange). The aggregate
     would double-count volume, so we keep the maker fills and drop it. Same
     policy as the RPC cleaner.

  2. `size` IS USDC, NOT SHARES.  <-- the subtle one
     `enrichedOrderFilled.size` is the COLLATERAL (USDC, 6-decimals) leg of the
     fill, not the share count. So:
         notional_usd = size / 1e6
         quantity     = (size / 1e6) / price      # shares
     Reading `size` as shares (and then multiplying by price for notional) gets
     BOTH columns wrong — it under-reports size by a factor of `price` and
     notional by `price^2`.

  3. `side` IS THE MAKER'S SIDE ON MAKER ROWS — flip it for taker perspective.
     Our schema defines `side` as the TAKER (aggressor) direction. On a maker-fill
     row the subgraph reports the MAKER's direction, which is the opposite.

HOW (2) AND (3) WERE ESTABLISHED (not guessed — project Rule 2):
  - Aggregate check: sum(size)/1e6 over ALL rows for a token reproduces the
    subgraph's own `orderbook.scaledCollateralVolume` to ratio 1.000. Collateral,
    i.e. USDC.
  - Chain check on tx 0x294d4e57…84fc, via the raw `orderFilledEvent` entity
    (which carries the on-chain makerAssetId/amounts):
      * a fill where the maker GAVE USDC:  on-chain $870 / 3,000 shares @0.29,
        and enriched `size` = 870,000,000 -> the USDC leg. shares = 870/0.29 = 3000.
      * a fill where the maker GAVE TOKENS: on-chain 500 shares / $355 @0.71,
        and enriched `size` = 355,000,000 -> STILL the USDC leg (not the 500
        shares). So `size` is always collateral, whichever side the maker gave.
  - Side check: for 434 trades matched by (tx, token) against Polymarket's public
    data-api, the taker's side equals FLIP(maker-row side) in 434/434 cases and
    the un-flipped value in 0/434.

Run (event-driven — condition/tokens/paths from events/<slug>/event.json):
    python src/clean_polymarket_subgraph.py mamdani-dem-nomination
"""

import csv
import json
import sys
from datetime import datetime, timezone

from eventlib import load_event, open_raw, raw_path

# Shared cross-venue schema (must stay identical in clean_kalshi_trades.py).
FIELDS = [
    "venue", "timestamp", "unix_ts", "trade_id", "outcome", "side",
    "price", "quantity", "notional_usd", "yes_price",
    "tx_hash", "maker", "taker", "is_block_trade",
]


# The subgraph labels a maker-fill row with the MAKER's direction; our schema
# wants the TAKER's. They are always opposites.
FLIP_SIDE = {"BUY": "SELL", "SELL": "BUY"}


def decode(rec: dict, exchange: str, token_name: dict) -> dict | None:
    """One enrichedOrderFilled record -> a trade row, or None if it's the aggregate."""
    taker = rec["taker"]["id"]
    if taker == exchange:
        return None  # aggregate taker-order row — drop (matches rpc policy)

    ts = int(rec["timestamp"])
    price = round(float(rec["price"]), 6)
    if price <= 0:
        return None  # cannot recover a share count from a zero price

    # `size` is the USDC (collateral) leg, 6-decimals — NOT the share count.
    notional_usd = int(rec["size"]) / 1e6
    quantity = notional_usd / price                      # shares

    outcome = token_name.get(rec["market"], rec["market"])
    yes_price = price if outcome == "YES" else round(1 - price, 6)
    return {
        "venue": "polymarket",
        "timestamp": datetime.fromtimestamp(ts, timezone.utc).isoformat(),
        "unix_ts": ts,
        "trade_id": rec["id"],               # "<txHash>_<orderHash>", unique
        "outcome": outcome,
        "side": FLIP_SIDE[rec["side"].upper()],   # maker's side -> taker's side
        "price": price,
        "quantity": round(quantity, 6),
        "notional_usd": round(notional_usd, 6),
        "yes_price": yes_price,
        "tx_hash": rec["transactionHash"],
        "maker": rec["maker"]["id"],
        "taker": taker,
        "is_block_trade": "",
    }


def main() -> None:
    ev = load_event(sys.argv[1] if len(sys.argv) > 1 else None)
    if len(ev.poly_token_ids) < 2:
        raise SystemExit("event.json needs polymarket_token_ids: [YES, NO]")
    exchange = ev.poly_exchange
    token_name = {ev.poly_token_ids[0]: "YES", ev.poly_token_ids[1]: "NO"}
    src = raw_path(ev.poly_subgraph_raw_jsonl, ev.poly_raw_gzip)
    clean_path = ev.poly_subgraph_clean_csv
    if not src.exists():
        raise SystemExit(f"Raw file not found: {src}")

    n_recs = n_agg = 0
    # Rows are held as TUPLES, not dicts: presidential-2024 yields ~2.75M clean
    # rows, and a dict per row (14 keys) costs several GB. Tuples in FIELDS order
    # cut that by roughly 3x and keep the sort in memory on a 16GB box.
    rows: list[tuple] = []
    with open_raw(ev.poly_subgraph_raw_jsonl, "rt", ev.poly_raw_gzip) as f:
        for line in f:
            n_recs += 1
            out = decode(json.loads(line), exchange, token_name)
            if out is None:
                n_agg += 1
            else:
                rows.append(tuple(out[k] for k in FIELDS))

    TS, TID = FIELDS.index("unix_ts"), FIELDS.index("trade_id")
    rows.sort(key=lambda r: (r[TS], r[TID]))

    with clean_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(FIELDS)
        w.writerows(rows)

    from collections import Counter
    OUT, PX, NOT = (FIELDS.index("outcome"), FIELDS.index("price"),
                    FIELDS.index("notional_usd"))
    by_outcome = Counter(r[OUT] for r in rows)
    vol = sum(r[NOT] for r in rows)
    print(f"subgraph records read : {n_recs:,}")
    print(f"  aggregate (dropped) : {n_agg:,}")
    print(f"clean trades written  : {len(rows):,}  -> {clean_path}")
    print(f"  by outcome          : {dict(by_outcome)}")
    if rows:
        ts_i = FIELDS.index("timestamp")
        print(f"  date range          : {rows[0][ts_i][:10]} .. {rows[-1][ts_i][:10]}")
        print(f"  price range         : {min(r[PX] for r in rows)} .. {max(r[PX] for r in rows)}")
        print(f"  total USDC volume   : ${vol:,.0f}")


if __name__ == "__main__":
    main()
