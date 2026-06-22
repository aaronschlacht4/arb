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

Run:
    python src/clean_polymarket_subgraph.py
"""

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"

CONDITION_ID = "0xebddfcf7b4401dade8b4031770a1ab942b01854f3bed453d5df9425cd9f211a9"
EXCHANGE = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
YES_TOKEN = "33945469250963963541781051637999677727672635213493648594066577298999471399137"
NO_TOKEN = "105832362350788616148612362642992403996714020918558917275151746177525518770551"
TOKEN_NAME = {YES_TOKEN: "YES", NO_TOKEN: "NO"}

RAW_PATH = RAW_DIR / f"polymarket_subgraph_orderfilled_{CONDITION_ID}.jsonl"
CLEAN_PATH = CLEAN_DIR / f"polymarket_subgraph_trades_{CONDITION_ID}.csv"

# Shared cross-venue schema (must stay identical in clean_kalshi_trades.py).
FIELDS = [
    "venue", "timestamp", "unix_ts", "trade_id", "outcome", "side",
    "price", "quantity", "notional_usd", "yes_price",
    "tx_hash", "maker", "taker", "is_block_trade",
]


def decode(rec: dict) -> dict | None:
    """One enrichedOrderFilled record -> a trade row, or None if it's the aggregate."""
    taker = rec["taker"]["id"]
    if taker == EXCHANGE:
        return None  # aggregate taker-order row — drop (matches rpc policy)

    ts = int(rec["timestamp"])
    price = round(float(rec["price"]), 6)
    quantity = int(rec["size"]) / 1e6  # size is a 6-decimal token amount
    outcome = TOKEN_NAME.get(rec["market"], rec["market"])
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
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    if not RAW_PATH.exists():
        raise SystemExit(f"Raw file not found: {RAW_PATH}")

    n_recs = n_agg = 0
    rows = []
    with RAW_PATH.open() as f:
        for line in f:
            n_recs += 1
            out = decode(json.loads(line))
            if out is None:
                n_agg += 1
            else:
                rows.append(out)

    rows.sort(key=lambda r: (r["unix_ts"], r["trade_id"]))

    with CLEAN_PATH.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    from collections import Counter
    by_outcome = Counter(r["outcome"] for r in rows)
    vol = sum(r["notional_usd"] for r in rows)
    print(f"subgraph records read : {n_recs:,}")
    print(f"  aggregate (dropped) : {n_agg:,}")
    print(f"clean trades written  : {len(rows):,}  -> {CLEAN_PATH}")
    print(f"  by outcome          : {dict(by_outcome)}")
    if rows:
        print(f"  date range          : {rows[0]['timestamp'][:10]} .. {rows[-1]['timestamp'][:10]}")
        print(f"  price range         : {min(r['price'] for r in rows)} .. {max(r['price'] for r in rows)}")
        print(f"  total USDC volume   : ${vol:,.0f}")


if __name__ == "__main__":
    main()
