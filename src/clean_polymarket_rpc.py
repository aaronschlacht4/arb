"""
Clean the RAW Polymarket OrderFilled logs into a trade-level CSV.

INPUT  (raw, never modified): events/<slug>/raw/polymarket_rpc_orderfilled_<cond>.jsonl
OUTPUT (derived, overwritable): events/<slug>/clean/polymarket_rpc_trades_<cond>.csv

Each raw line is one `OrderFilled` log (hex-encoded). The chain records only an
asset swap; "price" and "shares" are reconstructed here.

KEY DESIGN DECISIONS (verified empirically against the Goldsky subgraph):
  1. Each economic trade emits MULTIPLE OrderFilled logs:
       - one per matched MAKER order  (topics taker = the real taker address)
       - one AGGREGATE taker-order log (topics taker = the Exchange address)
     The aggregate is exactly the SUM of its maker fills, so keeping both would
     double-count volume. The clean tape KEEPS THE MAKER FILLS and DROPS the
     aggregate (taker == Exchange).
  2. One side of every fill is USDC (assetId 0); the other is the YES/NO token.
     All amounts are 6-decimal (divide by 1e6).
  3. side (taker/aggressor perspective): makerAssetId == USDC -> taker SELL ;
     takerAssetId == USDC -> taker BUY.
  4. price = USDC amount / token amount ; shares = token amount / 1e6.
  5. Emits a `log_index` column so the arb merge can build Wendy's `tx;logIndex`
     poly id and use it for the within-second consumption tie-break.

Run:
    python src/clean_polymarket_rpc.py mamdani-dem-nomination
"""

import csv
import json
import sys
from datetime import datetime, timezone

from eventlib import load_event

# Shared cross-venue schema + log_index (RPC exposes it; subgraph does not).
FIELDS = [
    "venue", "timestamp", "unix_ts", "trade_id", "log_index", "outcome", "side",
    "price", "quantity", "notional_usd", "yes_price",
    "tx_hash", "maker", "taker", "is_block_trade",
]


def addr(topic: str) -> str:
    return "0x" + topic[-40:]


def word(data_hex: str, i: int) -> int:
    h = data_hex[2:]
    return int(h[i * 64:(i + 1) * 64], 16)


def decode(log: dict, exchange: str, token_name: dict) -> dict | None:
    taker = addr(log["topics"][3])
    if taker == exchange:
        return None  # aggregate taker-order log — drop to avoid double-counting

    maker = addr(log["topics"][2])
    maker_asset = word(log["data"], 0)
    taker_asset = word(log["data"], 1)
    maker_amt = word(log["data"], 2)
    taker_amt = word(log["data"], 3)

    if maker_asset == 0:           # maker gave USDC -> taker SELLING
        usdc_raw, token_raw, token_id, taker_side = maker_amt, taker_amt, taker_asset, "SELL"
    elif taker_asset == 0:         # maker gave tokens -> taker BUYING
        usdc_raw, token_raw, token_id, taker_side = taker_amt, maker_amt, maker_asset, "BUY"
    else:
        return {"_anomaly": True, "tx_hash": log["transactionHash"],
                "log_index": int(log["logIndex"], 16)}

    ts = int(log["blockTimestamp"], 16)
    outcome = token_name.get(str(token_id), str(token_id))
    price = round(usdc_raw / token_raw, 6) if token_raw else None
    yes_price = price if (price is None or outcome == "YES") else round(1 - price, 6)
    return {
        "venue": "polymarket",
        "timestamp": datetime.fromtimestamp(ts, timezone.utc).isoformat(),
        "unix_ts": ts,
        "trade_id": log["transactionHash"],
        "log_index": int(log["logIndex"], 16),
        "outcome": outcome,
        "side": taker_side,
        "price": price,
        "quantity": token_raw / 1e6,
        "notional_usd": round(usdc_raw / 1e6, 6),
        "yes_price": yes_price,
        "tx_hash": log["transactionHash"],
        "maker": maker,
        "taker": taker,
        "is_block_trade": "",
    }


def main() -> None:
    ev = load_event(sys.argv[1] if len(sys.argv) > 1 else None)
    if len(ev.poly_token_ids) < 2:
        raise SystemExit("event.json needs polymarket_token_ids: [YES, NO]")
    exchange = ev.poly_exchange
    token_name = {ev.poly_token_ids[0]: "YES", ev.poly_token_ids[1]: "NO"}
    raw_path = ev.poly_rpc_raw_jsonl
    clean_path = ev.poly_rpc_clean_csv
    if not raw_path.exists():
        raise SystemExit(f"Raw file not found: {raw_path}")

    n_logs = n_agg = n_anom = 0
    rows = []
    with raw_path.open() as f:
        for line in f:
            n_logs += 1
            out = decode(json.loads(line), exchange, token_name)
            if out is None:
                n_agg += 1
            elif out.get("_anomaly"):
                n_anom += 1
            else:
                rows.append(out)

    rows.sort(key=lambda r: (r["unix_ts"], r["tx_hash"], r["log_index"]))

    with clean_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    from collections import Counter
    by_outcome = Counter(r["outcome"] for r in rows)
    vol = sum(r["notional_usd"] for r in rows)
    print(f"raw logs read         : {n_logs:,}")
    print(f"  aggregate (dropped) : {n_agg:,}")
    print(f"  anomalies (skipped) : {n_anom:,}")
    print(f"clean trades written  : {len(rows):,}  -> {clean_path}")
    print(f"  by outcome          : {dict(by_outcome)}")
    if rows:
        print(f"  date range          : {rows[0]['timestamp'][:10]} .. {rows[-1]['timestamp'][:10]}")
        print(f"  total USDC volume   : ${vol:,.0f}")


if __name__ == "__main__":
    main()
