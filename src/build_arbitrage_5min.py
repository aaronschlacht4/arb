"""
5-minute-window arbitrage view for the Mamdani market (Polymarket vs Kalshi).

Collapses the per-fill arbitrage log into clock-aligned 5-minute windows so each
row is ONE window, not one trade. Built for accuracy:

  - Fixed 5-minute clock windows (unix_ts // 300).
  - A window is only evaluated if BOTH venues actually traded in it, so both
    prices are real and contemporaneous (no carried-forward / stale prices and
    no reaching across windows).
  - Each venue's window price = VWAP (volume-weighted by quantity) of its trades
    in the window — a fair representative, not a cherry-picked best.
  - Arb exists iff the two YES prices differ: buy YES on the cheaper-YES venue +
    NO on the other. leg_sum = 1 - |poly_yes - kalshi_yes|; edge = |spread|.
  - arb_size = min(poly volume, kalshi volume) in the window = the size actually
    matchable on both legs (both units are $1-notional).
  - Window starts at the same nomination floor as the per-fill file (2025-06-25).

Columns: window_start, window_unix, poly_yes, poly_no, kalshi_yes, kalshi_no,
         edge, poly_vol, kalshi_vol, arb_size, poly_side, kalshi_side

Output: data/clean/arbitrage_5min.csv

Run:
    python src/build_arbitrage_5min.py
"""

import csv
import datetime as dt
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MERGED = ROOT / "data" / "clean" / "merged_trades_mamdani_by_timestamp.csv"
OUT = ROOT / "data" / "clean" / "arbitrage_5min.csv"

WINDOW_S = 300
START_TS = int(dt.datetime(2025, 6, 25, 2, 0, tzinfo=dt.timezone.utc).timestamp())


def main():
    # accumulate per (window, venue): sum(price*qty), sum(qty)
    acc = defaultdict(lambda: {"polymarket": [0.0, 0.0], "kalshi": [0.0, 0.0]})
    end = 0
    for r in csv.DictReader(MERGED.open(encoding="utf-8")):
        ts = int(r["unix_ts"])
        if ts < START_TS:
            continue
        end = max(end, ts)
        v = r["venue"]
        if v not in ("polymarket", "kalshi"):
            continue
        q = float(r["quantity"])
        y = float(r["yes_price"])
        w = ts // WINDOW_S * WINDOW_S
        acc[w][v][0] += y * q
        acc[w][v][1] += q

    rows = []
    for w in sorted(acc):
        p, k = acc[w]["polymarket"], acc[w]["kalshi"]
        if p[1] == 0 or k[1] == 0:
            continue  # need BOTH venues traded in the window for a real comparison
        # round to 6 dp FIRST, then decide — so the stored prices are exactly what
        # the arbitrage test uses (no window where the displayed legs sum to >= 1).
        p_yes = round(p[0] / p[1], 6)
        k_yes = round(k[0] / k[1], 6)
        edge = round(abs(p_yes - k_yes), 6)
        if edge <= 0:
            continue  # prices identical at stored precision -> not a real arbitrage
        # buy YES where YES is cheaper, NO on the other
        if p_yes < k_yes:
            p_side, k_side = "YES", "NO"
        else:
            p_side, k_side = "NO", "YES"
        rows.append({
            "window_start": dt.datetime.fromtimestamp(w, dt.timezone.utc).isoformat(),
            "window_unix": w,
            "poly_yes": p_yes,
            "poly_no": round(1 - p_yes, 6),
            "kalshi_yes": k_yes,
            "kalshi_no": round(1 - k_yes, 6),
            "edge": edge,
            "poly_vol": round(p[1], 4),
            "kalshi_vol": round(k[1], 4),
            "arb_size": round(min(p[1], k[1]), 4),
            "poly_side": p_side,
            "kalshi_side": k_side,
        })

    fields = ["window_start", "window_unix", "poly_yes", "poly_no", "kalshi_yes",
              "kalshi_no", "edge", "poly_vol", "kalshi_vol", "arb_size",
              "poly_side", "kalshi_side"]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    # windows where BOTH traded (denominator) for context
    both = sum(1 for wk in acc.values() if wk["polymarket"][1] and wk["kalshi"][1])
    print(f"Wrote {len(rows):,} arbitrage windows -> {OUT}")
    print(f"  windows where both venues traded: {both:,}  ({100*len(rows)/both:.0f}% had an arb)")
    if rows:
        prof = sum(r["edge"] * r["arb_size"] for r in rows)
        print(f"  theoretical profit (edge x matchable size): ${prof:,.0f}")
        print(f"  window span: {rows[0]['window_start']} -> {rows[-1]['window_start']}")


if __name__ == "__main__":
    main()
