"""
Clean the SUBGRAPH-sourced raw Polymarket fills into a trade-level CSV.

INPUT  (raw, never modified): data/raw/polymarket_subgraph_orderfilled_<conditionId>.jsonl
OUTPUT (derived, overwritable): data/clean/polymarket_subgraph_trades_<conditionId>.csv

Subgraph-method twin of `clean_polymarket_rpc.py`. The subgraph already DECODES
price/size/side/timestamp, so the only real transformation is the SAME aggregate
drop (taker == Exchange) the on-chain pipeline does. Emits the shared cross-venue
schema (see notes/data_dictionary_polymarket.md) so all clean tapes are identical.

NOTE on `side`: the subgraph reports its own Buy/Sell label; whether that matches
our TAKER-perspective convention is verified during the rpc-vs-subgraph
reconciliation (shared tx_hash). Left as the subgraph's value, upper-cased, until
then.

Run (event-driven — condition/tokens/paths from events/<slug>/event.json):
    python src/clean_polymarket_subgraph.py mamdani-dem-nomination
"""

import csv
import json
import sys
from datetime import datetime, timezone

from eventlib import load_event

# Shared cross-venue schema (must stay identical in clean_kalshi_trades.py).
FIELDS = [
    "venue", "timestamp", "unix_ts", "trade_id", "outcome", "side",
    "price", "quantity", "notional_usd", "yes_price",
    "tx_hash", "maker", "taker", "is_block_trade",
]


def decode(rec: dict, exchange: str, token_name: dict) -> dict | None:
    """One enrichedOrderFilled record -> a trade row, or None if it's the aggregate."""
    taker = rec["taker"]["id"]
    if taker == exchange:
        return None  # aggregate taker-order row — drop (matches rpc policy)

    ts = int(rec["timestamp"])
    price = round(float(rec["price"]), 6)
    quantity = int(rec["size"]) / 1e6  # size is a 6-decimal token amount
    outcome = token_name.get(rec["market"], rec["market"])
    yes_price = price if outcome == "YES" else round(1 - price, 6)
    return {
        "venue": "polymarket",
        "timestamp": datetime.fromtimestamp(ts, timezone.utc).isoformat(),
        "unix_ts": ts,
        "trade_id": rec["id"],               # "<txHash>_<orderHash>", unique
        "outcome": outcome,
        "side": rec["side"].upper(),         # subgraph-decoded; perspective TBD (see note)
        "price": price,
        "quantity": quantity,
        "notional_usd": round(price * quantity, 6),
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
    raw_path = ev.poly_subgraph_raw_jsonl
    clean_path = ev.poly_subgraph_clean_csv
    if not raw_path.exists():
        raise SystemExit(f"Raw file not found: {raw_path}")

    n_recs = n_agg = 0
    rows = []
    with raw_path.open() as f:
        for line in f:
            n_recs += 1
            out = decode(json.loads(line), exchange, token_name)
            if out is None:
                n_agg += 1
            else:
                rows.append(out)

    rows.sort(key=lambda r: (r["unix_ts"], r["trade_id"]))

    with clean_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    from collections import Counter
    by_outcome = Counter(r["outcome"] for r in rows)
    vol = sum(r["notional_usd"] for r in rows)
    print(f"subgraph records read : {n_recs:,}")
    print(f"  aggregate (dropped) : {n_agg:,}")
    print(f"clean trades written  : {len(rows):,}  -> {clean_path}")
    print(f"  by outcome          : {dict(by_outcome)}")
    if rows:
        print(f"  date range          : {rows[0]['timestamp'][:10]} .. {rows[-1]['timestamp'][:10]}")
        print(f"  price range         : {min(r['price'] for r in rows)} .. {max(r['price'] for r in rows)}")
        print(f"  total USDC volume   : ${vol:,.0f}")


if __name__ == "__main__":
    main()
