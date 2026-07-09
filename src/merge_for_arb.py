"""
Merge the two clean tapes (Kalshi + Polymarket) into the SORTED per-trade input
the arbitrage matcher consumes, i.e. Wendy's `kalshi_poly_merged_sorted_for_arb`
schema:

    timestamp,id,market,side,size,yes_price,no_price

Prep applied here (mirrors arbitrage.py's PRE-ARB step):
  - Polymarket sizes are FLOORED to whole contracts; anything that floors to 0
    is dropped (no liquidity, no row).
  - Polymarket `yes_price` is rounded to 3 dp (the value the arb test compares).
  - Kalshi `side` = outcome lower-cased; Polymarket `side` = outcome upper-cased.
  - Polymarket id = `<tx>;<logIndex>` when the tape has a log_index column
    (RPC source), else the tape's unique trade_id as-is (subgraph source).
  - Rows sorted by (timestamp, id).

Run:
    python src/merge_for_arb.py mamdani-dem-nomination
"""
import csv
import sys

from eventlib import load_event


def poly_id(r: dict) -> str:
    tid = r["trade_id"]
    li = r.get("log_index")
    if li not in (None, ""):
        tx = tid[2:] if tid.startswith("0x") else tid
        return f"{tx};{li}"
    return tid  # subgraph: "<tx>_<orderHash>", already unique


def main() -> None:
    ev = load_event(sys.argv[1] if len(sys.argv) > 1 else None)
    rows = []

    # --- Kalshi ---
    with ev.kalshi_clean_csv.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            yes = float(r["yes_price"])
            rows.append({
                "timestamp": int(r["unix_ts"]),
                "id": r["trade_id"],
                "market": "kalshi",
                "side": r["outcome"].lower(),
                "size": float(r["quantity"]),
                "yes_price": yes,
                "no_price": round(1.0 - yes, 6),  # avoid float artifacts (0.92999…)
            })

    # --- Polymarket (prefer RPC tape for true logIndex; else subgraph) ---
    poly_path = (ev.poly_rpc_clean_csv if ev.poly_rpc_clean_csv.exists()
                 else ev.poly_subgraph_clean_csv)
    if not poly_path.exists():
        raise SystemExit(f"No Polymarket clean tape found for {ev.slug}")
    n_dropped = 0
    with poly_path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            size = int(float(r["quantity"]))  # floor to whole contracts
            if size <= 0:
                n_dropped += 1
                continue
            # Wendy's prep rounds poly yes to 3dp BEFORE the arb test — this is
            # part of the verified methodology (chain prices like 0.90899999842
            # must compare as 0.909, else the |gap| >= 1e-4 test misfires).
            yes = round(float(r["yes_price"]), 3)
            rows.append({
                "timestamp": int(r["unix_ts"]),
                "id": poly_id(r),
                "market": "poly",
                "side": r["outcome"].upper(),
                "size": float(size),
                "yes_price": yes,
                "no_price": round(1.0 - yes, 3),
            })

    rows.sort(key=lambda x: (x["timestamp"], str(x["id"])))
    fields = ["timestamp", "id", "market", "side", "size", "yes_price", "no_price"]
    with ev.merged_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    nk = sum(1 for r in rows if r["market"] == "kalshi")
    npoly = sum(1 for r in rows if r["market"] == "poly")
    src = "rpc" if poly_path == ev.poly_rpc_clean_csv else "subgraph"
    print(f"Merged {len(rows):,} rows -> {ev.merged_csv}")
    print(f"  kalshi: {nk:,}   poly ({src}): {npoly:,}   (poly floored-to-0 dropped: {n_dropped:,})")


if __name__ == "__main__":
    main()
