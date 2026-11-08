"""
Compare two events that cover the SAME market pair from DIFFERENT data sources.

Polymarket's V2 migration forced a re-collection of every event from a new
source with coarser row granularity (one row per taker order, where the old
subgraph gave one row per maker fill). The question that then matters is not
"did the number change" — it will have — but "did it change by more than the
granularity explains". This script answers that in one place instead of being
re-derived by hand each time.

    python arb.py compare presidential-2024 presidential-2024-v2

WHAT IT COMPARES, and why each line is the one worth looking at:

  CONFIG      kalshi ticker, condition id, window, start/end cutoffs.
              Checked FIRST and loudly, because a difference here invalidates
              every other row: the two events are then not the same experiment.
              A mismatched start_ts in particular can move the Net by double
              digits (see notes/polymarket_v2_migration.md).

  TAPES       rows and share volume per venue.
              Rows are EXPECTED to differ roughly 2x across sources and mean
              nothing on their own. SHARE VOLUME is the real invariant: the
              same trades carry the same shares however they are grouped, so a
              volume gap above ~1% means one tape is incomplete, not merely
              coarser.

  ARBITRAGE   matches, matched trades, and the Net.
              The Net moves with granularity because the matcher depletes whole
              rows; chunkier rows match differently. On the two Mamdani events
              that showed up as -0.41% and +0.97%. A Net delta far outside the
              share-volume delta is the signal that something other than
              granularity changed.

Exit status is 0 when the two runs agree within tolerance, 1 when they do not,
so this can gate a re-collection.
"""

import argparse
import csv
import sys

from arbpipe.eventlib import load_event
from arbpipe.verify.validate import gamma_volume_shares

OK, BAD, WARN = "  [ok]  ", "  [FAIL]", "  [warn]"

# Share volume is an invariant across sources; 1% is the same tolerance
# validate_event.py uses against Gamma's independent counter.
VOLUME_TOL = 0.01
# The Net legitimately moves with row granularity. 5% is generous for that and
# still far tighter than the gap a truncated tape or a wrong window produces.
NET_TOL = 0.05


def tape_stats(path) -> dict:
    """rows + share volume for one clean tape, streamed."""
    n = 0
    shares = 0.0
    notional = 0.0
    first = last = None
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            n += 1
            shares += float(r["quantity"])
            notional += float(r["notional_usd"])
            if first is None:
                first = r["timestamp"][:10]
            last = r["timestamp"][:10]
    return {"rows": n, "shares": shares, "notional": notional,
            "first": first, "last": last}


def count_rows(path) -> int | None:
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as f:
        return max(0, sum(1 for _ in f) - 1)


def read_net(ev) -> float | None:
    p = ev.results_dir / "results.txt"
    if not p.exists():
        return None
    for line in p.read_text().splitlines():
        if line.startswith("Net:"):
            return float(line.split(":", 1)[1])
    return None


def poly_source(ev) -> str:
    path = ev.poly_clean_csv()
    return {ev.poly_rpc_clean_csv: "rpc",
            ev.poly_dataapi_clean_csv: "dataapi",
            ev.poly_subgraph_clean_csv: "subgraph"}.get(path, "none")


def pct(a: float | None, b: float | None) -> str:
    """b relative to a, as a signed percentage."""
    if a in (None, 0) or b is None:
        return "    n/a"
    return f"{(b - a) / abs(a) * 100:+7.2f}%"


def row(label: str, a, b, fmt: str = ",.0f") -> None:
    sa = format(a, fmt) if isinstance(a, (int, float)) else str(a or "-")
    sb = format(b, fmt) if isinstance(b, (int, float)) else str(b or "-")
    delta = pct(a, b) if isinstance(a, (int, float)) and isinstance(b, (int, float)) else ""
    print(f"  {label:<26}{sa:>18}{sb:>18}  {delta}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Compare two events covering the same market pair from "
                    "different data sources.")
    ap.add_argument("baseline", help="event slug collected FIRST (e.g. the "
                                     "subgraph-sourced run)")
    ap.add_argument("candidate", help="event slug to check against it")
    args = ap.parse_args()

    a, b = load_event(args.baseline), load_event(args.candidate)
    fails: list[str] = []

    print(f"\n{'=' * 66}")
    print(f"COMPARE  {a.slug}  ->  {b.slug}")
    print("=" * 66)
    print(f"  {'':<26}{args.baseline[:18]:>18}{args.candidate[:18]:>18}")
    print(f"  {'poly source':<26}{poly_source(a):>18}{poly_source(b):>18}")

    # --- CONFIG: anything different here invalidates the rest --------------
    print(f"\n-- CONFIG --")
    for label, va, vb in (
            ("kalshi_ticker", a.kalshi_ticker, b.kalshi_ticker),
            ("condition_id", a.poly_condition_id, b.poly_condition_id),
            ("window_seconds", a.window_seconds, b.window_seconds),
            ("start_ts", a.start_ts, b.start_ts),
            ("end_ts", a.end_ts, b.end_ts),
            ("resolution", a.resolution, b.resolution)):
        same = va == vb
        shown_a = str(va)[-18:] if va is not None else "-"
        shown_b = str(vb)[-18:] if vb is not None else "-"
        print(f"{OK if same else BAD} {label:<18}{shown_a:>20}{shown_b:>20}")
        if not same:
            fails.append(
                f"config differs: {label} {va!r} vs {vb!r} — these two runs "
                f"are not the same experiment, so the numbers below are not "
                f"comparable")

    # --- TAPES -------------------------------------------------------------
    print(f"\n-- CLEAN TAPES --")
    print(f"  {'':<26}{'baseline':>18}{'candidate':>18}   delta")
    for venue, pa, pb in (("kalshi", a.kalshi_clean_csv, b.kalshi_clean_csv),
                          ("polymarket", a.poly_clean_csv(), b.poly_clean_csv())):
        if pa is None or pb is None or not pa.exists() or not pb.exists():
            print(f"  {venue}: tape missing on one side — skipping")
            continue
        sa, sb = tape_stats(pa), tape_stats(pb)
        row(f"{venue} rows", sa["rows"], sb["rows"])
        row(f"{venue} shares", sa["shares"], sb["shares"])
        print(f"  {venue + ' dates':<26}{sa['first'] + '..' + sa['last']:>18}"
              f"{sb['first'] + '..' + sb['last']:>18}")
        # Share volume is the invariant — rows are not.
        if sa["shares"]:
            drift = abs(sb["shares"] - sa["shares"]) / sa["shares"]
            if drift > VOLUME_TOL:
                # Direction is the diagnosis, same as in validate_event.py: a
                # tape can only be SHORT of volume by missing trades, never
                # over by missing them. Reporting both as "incomplete" sends
                # the reader looking for a truncation that is not there.
                if sb["shares"] < sa["shares"]:
                    why = (f"candidate is MISSING {drift * 100:.2f}% of the "
                           f"baseline's volume — likely an incomplete fetch")
                else:
                    why = (f"candidate EXCEEDS the baseline by {drift * 100:.2f}% "
                           f"— not a short fetch; check both against Gamma "
                           f"(validate_event.py) to see WHICH side is off")
                print(f"{BAD} {venue}: share volume differs by {drift * 100:.2f}% "
                      f"(> {VOLUME_TOL * 100:.0f}%)")
                print(f"         -> {why}")
                fails.append(f"{venue} share volume drift {drift * 100:.2f}% — {why}")
                # Break the tie with an INDEPENDENT oracle rather than leaving
                # the reader to assume the baseline is right. Gamma computes
                # its share counter from its own indexer, so whichever tape it
                # agrees with is the one to trust. This matters concretely:
                # presidential-2024's subgraph tape runs 2.43% ABOVE Gamma, so
                # a v2 tape that merely matches Gamma would look "short" here
                # while actually being the correct one.
                if venue == "polymarket":
                    gv = gamma_volume_shares(a.poly_condition_id)
                    if gv:
                        ra, rb = sa["shares"] / gv, sb["shares"] / gv
                        print(f"         Gamma volumeNum : {gv:,.0f}")
                        print(f"           baseline/Gamma : {ra:.4f}")
                        print(f"           candidate/Gamma: {rb:.4f}")
                        closer = "candidate" if abs(rb - 1) < abs(ra - 1) else "baseline"
                        print(f"         -> the {closer.upper()} is closer to the "
                              f"independent counter; trust that one")
            else:
                print(f"{OK} {venue}: share volume agrees to "
                      f"{drift * 100:.3f}% — same trades, different grouping")

    # --- ARBITRAGE ---------------------------------------------------------
    print(f"\n-- ARBITRAGE --")
    print(f"  {'':<26}{'baseline':>18}{'candidate':>18}   delta")
    row("matches", count_rows(a.arb_final_csv), count_rows(b.arb_final_csv))
    row("matched trades", count_rows(a.matched_trades_csv),
        count_rows(b.matched_trades_csv))
    na, nb = read_net(a), read_net(b)
    row("Net (USD)", na, nb, ",.0f")

    if na is not None and nb is not None and na:
        d = abs(nb - na) / abs(na)
        if d > NET_TOL:
            print(f"{BAD} Net moved {d * 100:.2f}% (> {NET_TOL * 100:.0f}%) — "
                  f"more than row granularity explains; check the config rows "
                  f"above and both tapes' completeness")
            fails.append(f"Net drift {d * 100:.2f}%")
        else:
            print(f"{OK} Net moved {d * 100:.2f}% — consistent with the row "
                  f"granularity change")

    print("\n" + "=" * 66)
    if fails:
        print(f"COMPARISON FAILED — {len(fails)} problem(s):")
        for x in fails:
            print(f"  - {x}")
        sys.exit(1)
    print("COMPARISON OK — the two runs agree within tolerance.")
    print("NOTE: agreement here means the re-collection is sound. It does NOT")
    print("      make either Net realised P&L — both are upper bounds (see")
    print("      docs/arbitrage_matching_methodology.md).")


if __name__ == "__main__":
    main()
