"""
Merge the two clean tapes (Kalshi + Polymarket) into the SORTED per-trade input
the arbitrage matcher consumes. Every file written here is BYTE-IDENTICAL to
the file of the same name in Wendy's run when given her inputs; that is checked
by tests/test_wendy_parity.py and `python arb.py parity`.

WRITES FOUR FILES (all in events/<slug>/3_sorted/):
    merged_sorted_for_arb.csv              the interleaved tape (matcher input)
    kalshi_poly_merged_sorted_for_arb.csv   same content, Wendy's filename
    kalshi_sorted_for_arb.csv               Kalshi's tape alone
    poly_sorted_for_arb.csv                 Polymarket's tape alone

SCHEMAS (Wendy's, column for column):
    merged   timestamp,id,market,side,size,yes_price,no_price
    kalshi   kalshi_time,kalshi_yes,kalshi_no,kalshi_size,kalshi_trade_id,
             kalshi_side,_orig_idx,kalshi_size_orig,kalshi_yes_key
    poly     tx,logIndex,poly_time,poly_yes,poly_no,poly_yes_key,poly_size,
             poly_size_orig,outcome,_orig_idx,id

PREP (mirrors Wendy's pre-arb step):
  - WINDOW. Rows outside [start_ts, end_ts] are dropped here, so every file is
    the windowed tape. (The matcher applies the same cutoff again; harmless.)
  - Polymarket sizes are FLOORED to whole contracts. Rows that floor to 0 are
    KEPT, as in her files; the matcher skips size <= 0, so they never match.
  - Polymarket `yes_price` is rounded to 3 dp (the value the arb test compares).
  - Kalshi side = outcome lower-cased; Polymarket side = outcome upper-cased.
  - Polymarket id = `<tx>;<logIndex>` when the tape has a log_index column
    (RPC source), else the tape's unique trade_id as-is (Data API / subgraph).

ROW ORDER:
  - merged: (timestamp, str(id)).
  - kalshi: (timestamp, trade_id).
  - poly:   (timestamp, tx, int(logIndex)), the matcher's consumption order;
            (timestamp, id) for sources without a log index.

_orig_idx is Wendy's row number, assigned on the FULL tape before the window
cut, and it is what orders matched_trades.csv within a second:
  - kalshi: position in the full tape sorted by (timestamp, trade_id).
  - poly:   position in the full tape (zero-size rows included) sorted by
            (timestamp, "<tx>-<maker>-<taker>") as a string, stable on
            (timestamp, tx, logIndex). Verified against all 28,776 rows of her
            Mamdani-primary tape. Sources without maker/taker sort by id.

Run:
    python arb.py merge mamdani-dem-nomination
"""
import csv
import sys

from arbpipe.eventlib import load_event


def _strip0x(s: str) -> str:
    return s[2:] if s.startswith("0x") else s


def poly_id(r: dict) -> str:
    tid = r["trade_id"]
    li = r.get("log_index")
    if li not in (None, ""):
        return f"{_strip0x(tid)};{li}"
    return tid  # Data API / subgraph ids are already unique


def _poly_orig_key(r: dict) -> tuple:
    """Wendy's numbering key: (ts, 'tx-maker-taker') for RPC fills."""
    if r["log_index"] != "":
        return (r["timestamp"], f"{r['tx']}-{r['maker']}-{r['taker']}")
    return (r["timestamp"], r["id"])


def _in_window(ts: int, start, end) -> bool:
    return (start is None or ts >= start) and (end is None or ts <= end)


def load_kalshi(ev) -> list[dict]:
    rows = []
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
    rows.sort(key=lambda x: (x["timestamp"], x["id"]))
    for i, r in enumerate(rows):
        r["_orig_idx"] = i
    return rows


def load_poly(ev, poly_path) -> list[dict]:
    rows = []
    with poly_path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            # Wendy's prep rounds poly yes to 3dp BEFORE the arb test. This is
            # part of the verified methodology: chain prices like 0.90899999842
            # must compare as 0.909, else the |gap| >= 1e-4 test misfires.
            yes = round(float(r["yes_price"]), 3)
            li = r.get("log_index") or ""
            tx = _strip0x(r.get("tx_hash") or r["trade_id"]).lower() if li else r["trade_id"]
            rows.append({
                "timestamp": int(r["unix_ts"]),
                "id": poly_id(r),
                "market": "poly",
                "side": r["outcome"].upper(),
                "size": int(float(r["quantity"])),  # floor to whole contracts
                "yes_price": yes,
                "no_price": round(1.0 - yes, 3),
                "tx": tx,
                "log_index": li,
                "maker": (r.get("maker") or "").lower(),
                "taker": (r.get("taker") or "").lower(),
            })
    # Stable sort: the base order is the matcher's (ts, tx, int logIndex).
    rows.sort(key=lambda x: (x["timestamp"], x["tx"],
                             int(x["log_index"]) if x["log_index"] else 0))
    for i, r in enumerate(sorted(rows, key=_poly_orig_key)):
        r["_orig_idx"] = i
    return rows


def write_kalshi_sorted(path, rows: list[dict]) -> None:
    fields = ["kalshi_time", "kalshi_yes", "kalshi_no", "kalshi_size",
              "kalshi_trade_id", "kalshi_side", "_orig_idx",
              "kalshi_size_orig", "kalshi_yes_key"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(fields)
        for r in rows:
            w.writerow([r["timestamp"], r["yes_price"], r["no_price"], r["size"],
                        r["id"], r["side"], r["_orig_idx"], r["size"],
                        int(round(r["yes_price"] * 1000))])


def write_poly_sorted(path, rows: list[dict]) -> None:
    fields = ["tx", "logIndex", "poly_time", "poly_yes", "poly_no",
              "poly_yes_key", "poly_size", "poly_size_orig", "outcome",
              "_orig_idx", "id"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(fields)
        for r in rows:
            w.writerow([r["tx"], r["log_index"], r["timestamp"], r["yes_price"],
                        r["no_price"], int(round(r["yes_price"] * 1000)),
                        r["size"], r["size"], r["side"], r["_orig_idx"], r["id"]])


def main() -> None:
    ev = load_event(sys.argv[1] if len(sys.argv) > 1 else None)
    start, end = ev.start_ts, ev.end_ts

    poly_path = ev.poly_clean_csv()  # rpc > dataapi > subgraph
    if poly_path is None:
        raise SystemExit(
            f"No Polymarket clean tape found for {ev.slug}. Run:\n"
            f"  python arb.py fetch-poly {ev.slug}\n"
            f"  python arb.py clean-poly {ev.slug}")

    # Number on the FULL tapes, then cut to the window.
    K = [r for r in load_kalshi(ev) if _in_window(r["timestamp"], start, end)]
    P = [r for r in load_poly(ev, poly_path) if _in_window(r["timestamp"], start, end)]

    merged = sorted(K + P, key=lambda x: (x["timestamp"], str(x["id"])))
    fields = ["timestamp", "id", "market", "side", "size", "yes_price", "no_price"]
    for path in (ev.merged_csv, ev.merged_sorted_alias_csv):
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(fields)
            for r in merged:
                w.writerow([r["timestamp"], r["id"], r["market"], r["side"],
                            float(r["size"]), r["yes_price"], r["no_price"]])

    write_kalshi_sorted(ev.kalshi_sorted_csv, K)
    write_poly_sorted(ev.poly_sorted_csv, P)

    n_zero = sum(1 for r in P if r["size"] <= 0)
    src = {ev.poly_rpc_clean_csv: "rpc",
           ev.poly_dataapi_clean_csv: "dataapi",
           ev.poly_subgraph_clean_csv: "subgraph"}.get(poly_path, "?")
    print(f"Merged {len(merged):,} rows -> {ev.merged_csv}")
    print(f"  window [{start}, {end}]   kalshi: {len(K):,}   poly ({src}): {len(P):,}"
          f"   (poly floored to 0, kept: {n_zero:,})")
    print(f"  per-venue: {ev.kalshi_sorted_csv.name} ({len(K):,}), "
          f"{ev.poly_sorted_csv.name} ({len(P):,})")
    print(f"  alias:     {ev.merged_sorted_alias_csv.name}")


if __name__ == "__main__":
    main()
