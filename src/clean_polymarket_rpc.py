"""
Clean the RAW Polymarket OrderFilled logs into a trade-level CSV.

INPUT  (raw, never modified): data/raw/polymarket_rpc_orderfilled_<conditionId>.jsonl
OUTPUT (derived, overwritable): data/clean/polymarket_rpc_trades_<conditionId>.csv

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
  3. side (order-owner / maker perspective, matching the subgraph):
       makerAssetId == outcome token -> SELL ;  makerAssetId == USDC -> BUY
  4. price = USDC amount / token amount ; shares = token amount / 1e6.

Run:
    python src/clean_polymarket_rpc.py
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
YES_TOKEN = 33945469250963963541781051637999677727672635213493648594066577298999471399137
NO_TOKEN = 105832362350788616148612362642992403996714020918558917275151746177525518770551
TOKEN_NAME = {YES_TOKEN: "YES", NO_TOKEN: "NO"}

RAW_PATH = RAW_DIR / f"polymarket_rpc_orderfilled_{CONDITION_ID}.jsonl"
CLEAN_PATH = CLEAN_DIR / f"polymarket_rpc_trades_{CONDITION_ID}.csv"

# Shared cross-venue schema (must stay identical in clean_kalshi_trades.py).
# `side` is TAKER (aggressor) perspective; `yes_price` is the canonical implied
# P(event) (= price if outcome is YES, else 1-price), the column to compare
# against Kalshi. is_block_trade is Kalshi-only and left blank here.
FIELDS = [
    "venue", "timestamp", "unix_ts", "trade_id", "outcome", "side",
    "price", "quantity", "notional_usd", "yes_price",
    "tx_hash", "maker", "taker", "is_block_trade",
]


def addr(topic: str) -> str:
    """An indexed address topic is a 32-byte word; the address is the last 20 bytes."""
    return "0x" + topic[-40:]


def word(data_hex: str, i: int) -> int:
    """Return the i-th 32-byte word of a log `data` field as an int."""
    h = data_hex[2:]  # strip 0x
    return int(h[i * 64:(i + 1) * 64], 16)


def decode(log: dict) -> dict | None:
    """
    Decode one raw OrderFilled log into a trade row, or return None if it's the
    aggregate taker-order log (which we drop to avoid double-counting).
    """
    taker = addr(log["topics"][3])
    if taker == EXCHANGE:
        return None  # aggregate side — drop

    maker = addr(log["topics"][2])
    maker_asset = word(log["data"], 0)
    taker_asset = word(log["data"], 1)
    maker_amt = word(log["data"], 2)
    taker_amt = word(log["data"], 3)

    # Identify which side is the outcome token vs USDC (assetId 0). `taker_side`
    # is the TAKER (aggressor) perspective: when the maker BUYS the outcome, the
    # taker SELLS it, and vice-versa.
    if maker_asset == 0:           # maker gave USDC -> maker BUYING -> taker SELLING
        usdc_raw, token_raw, token_id, taker_side = maker_amt, taker_amt, taker_asset, "SELL"
    elif taker_asset == 0:         # maker gave outcome tokens -> maker SELLING -> taker BUYING
        usdc_raw, token_raw, token_id, taker_side = taker_amt, maker_amt, maker_asset, "BUY"
    else:
        # Neither side is USDC — not a normal binary-market trade; skip & flag.
        return {"_anomaly": True, "tx_hash": log["transactionHash"], "log_index": log["logIndex"]}

    ts = int(log["blockTimestamp"], 16)
    outcome = TOKEN_NAME.get(token_id, str(token_id))
    # price units cancel (both /1e6), so divide the raw integers directly.
    price = round(usdc_raw / token_raw, 6) if token_raw else None
    yes_price = price if (price is None or outcome == "YES") else round(1 - price, 6)
    return {
        "venue": "polymarket",
        "timestamp": datetime.fromtimestamp(ts, timezone.utc).isoformat(),
        "unix_ts": ts,
        "trade_id": f'{log["transactionHash"]}:{int(log["logIndex"], 16)}',
        "outcome": outcome,
        "side": taker_side,
        "price": price,
        "quantity": token_raw / 1e6,
        "notional_usd": round(usdc_raw / 1e6, 6),
        "yes_price": yes_price,
        "tx_hash": log["transactionHash"],
        "maker": maker,
        "taker": taker,
        "is_block_trade": "",   # n/a for Polymarket (on-chain venue)
    }


def main() -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    if not RAW_PATH.exists():
        raise SystemExit(f"Raw file not found: {RAW_PATH}")

    n_logs = n_agg = n_anom = 0
    rows = []
    with RAW_PATH.open() as f:
        for line in f:
            n_logs += 1
            out = decode(json.loads(line))
            if out is None:
                n_agg += 1
            elif out.get("_anomaly"):
                n_anom += 1
            else:
                rows.append(out)

    # Sort chronologically (raw is in block order already, but be explicit).
    rows.sort(key=lambda r: (r["unix_ts"], r["trade_id"]))

    with CLEAN_PATH.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    # --- summary / validation hints ---
    from collections import Counter
    by_outcome = Counter(r["outcome"] for r in rows)
    vol = sum(r["notional_usd"] for r in rows)
    print(f"raw logs read         : {n_logs:,}")
    print(f"  aggregate (dropped) : {n_agg:,}")
    print(f"  anomalies (skipped) : {n_anom:,}")
    print(f"clean trades written  : {len(rows):,}  -> {CLEAN_PATH}")
    print(f"  by outcome          : {dict(by_outcome)}")
    if rows:
        print(f"  date range          : {rows[0]['timestamp'][:10]} .. {rows[-1]['timestamp'][:10]}")
        print(f"  price range         : {min(r['price'] for r in rows)} .. {max(r['price'] for r in rows)}")
        print(f"  total USDC volume   : ${vol:,.0f}")


if __name__ == "__main__":
    main()
