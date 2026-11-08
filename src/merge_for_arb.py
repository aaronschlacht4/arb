"""
Merge the two clean tapes (Kalshi + Polymarket) into the SORTED per-trade input
the arbitrage matcher consumes, i.e. Wendy's `kalshi_poly_merged_sorted_for_arb`
schema:

    timestamp,id,market,side,size,yes_price,no_price

WRITES FOUR FILES (all in events/<slug>/results/):
    merged_sorted_for_arb.csv              the interleaved tape (matcher input)
    kalshi_poly_merged_sorted_for_arb.csv   same content, reference's filename
    kalshi_sorted_for_arb.csv               Kalshi's tape alone
    poly_sorted_for_arb.csv                 Polymarket's tape alone

The two PER-VENUE files are the reference run's pre-arb views, kept so a run can
be diffed against `reference/` and `results_to_compare/` column-for-column:

    id, <venue>_time, <venue>_yes, <venue>_no, <venue>_yes_key,
    <venue>_size, <venue>_size_orig, side, _orig_idx

  <venue>_yes_key  round(yes * 1000) as an int — the COMPARISON key only. The
                   matcher's own `abs(poly_yes - kalshi_yes) >= 1e-4` test is
                   equivalent to `yes_key` inequality at 3dp, and the key is
                   what makes that equivalence visible. Recorded prices are
                   NOT changed by it.
  <venue>_size     size the matcher starts from (Polymarket floored to whole
                   contracts); `_size_orig` preserves it for matched_trades.csv,
                   which reports the ORIGINAL size of each consumed trade, not
                   the amount consumed.
  _orig_idx        row position WITHIN THIS FILE, matching the reference run's
                   column of the same name.

                   It is NOT the matcher's internal index, and must not be
                   joined against one. Two reasons, both verified:
                     - run_arbitrage.py drops rows outside [start_ts, end_ts]
                       BEFORE numbering, while this file numbers the unfiltered
                       tape. On mamdani-nyc-mayor that is an offset of 1,480
                       rows (poly) and 1,628 (kalshi); 4 of 7 events have
                       cutoffs.
                     - for an RPC-sourced tape the matcher orders by
                       (ts, tx, int(logIndex)) whereas this file orders by
                       (ts, str(id)), and "…;10" sorts before "…;2" as a string.
                   Nothing published depends on this — matched_trades.csv
                   carries no _orig_idx to join on — but do not add one without
                   reconciling the two orderings first.

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


def write_venue_sorted(path, venue: str, rows: list[dict]) -> None:
    """One venue's pre-arb tape, in the reference run's per-venue schema.

    `rows` arrive sorted by (timestamp, str(id)) — this file's own order, which
    is NOT necessarily the matcher's consumption order. `_orig_idx` records
    position in THIS file only; see the module docstring for why it must not be
    joined against the matcher's index.
    """
    fields = [
        "id", f"{venue}_time", f"{venue}_yes", f"{venue}_no",
        f"{venue}_yes_key", f"{venue}_size", f"{venue}_size_orig",
        "side", "_orig_idx",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        w.writeheader()
        for i, r in enumerate(rows):
            w.writerow({
                "id": r["id"],
                f"{venue}_time": r["timestamp"],
                f"{venue}_yes": r["yes_price"],
                f"{venue}_no": r["no_price"],
                # Comparison key only — round(yes*1000). Does not alter prices.
                f"{venue}_yes_key": int(round(r["yes_price"] * 1000)),
                f"{venue}_size": r["size"],
                # Pre-arb these are equal; the matcher depletes `size` and
                # reports `size_orig`, so both columns exist from the start.
                f"{venue}_size_orig": r["size"],
                "side": r["side"],
                "_orig_idx": i,
            })


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

    # --- Polymarket (rpc > dataapi > subgraph; see eventlib.poly_clean_csv) ---
    poly_path = ev.poly_clean_csv()
    if poly_path is None:
        raise SystemExit(
            f"No Polymarket clean tape found for {ev.slug}. Run:\n"
            f"  python src/fetch_polymarket_dataapi.py {ev.slug}\n"
            f"  python src/clean_polymarket_dataapi.py {ev.slug}")
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
    for path in (ev.merged_csv, ev.merged_sorted_alias_csv):
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)

    # --- per-venue sorted tapes (reference pre-arb schema; see docstring) ---
    write_venue_sorted(ev.kalshi_sorted_csv, "kalshi",
                       [r for r in rows if r["market"] == "kalshi"])
    write_venue_sorted(ev.poly_sorted_csv, "poly",
                       [r for r in rows if r["market"] == "poly"])

    nk = sum(1 for r in rows if r["market"] == "kalshi")
    npoly = sum(1 for r in rows if r["market"] == "poly")
    src = {ev.poly_rpc_clean_csv: "rpc",
           ev.poly_dataapi_clean_csv: "dataapi",
           ev.poly_subgraph_clean_csv: "subgraph"}.get(poly_path, "?")
    print(f"Merged {len(rows):,} rows -> {ev.merged_csv}")
    print(f"  kalshi: {nk:,}   poly ({src}): {npoly:,}   (poly floored-to-0 dropped: {n_dropped:,})")
    print(f"  per-venue: {ev.kalshi_sorted_csv.name} ({nk:,}), "
          f"{ev.poly_sorted_csv.name} ({npoly:,})")
    print(f"  alias:     {ev.merged_sorted_alias_csv.name}")


if __name__ == "__main__":
    main()
