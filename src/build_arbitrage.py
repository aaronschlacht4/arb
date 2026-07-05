"""
Build the cross-venue arbitrage log for the Mamdani NYC-mayor market from
merged_trades_mamdani_by_timestamp.csv.

Arbitrage = buy YES on one venue and NO on the other for the SAME event; if the
two prices sum to < $1 the pair pays a guaranteed $1, locking in (1 - sum) profit.

Method (per the agreed rules, reverse-engineered from the target screenshot):
  - Comparison window = OVERLAP only: from the later of the two venues' first
    trade to the earlier of their last trade, BUT not earlier than the moment the
    two markets became the SAME event. Kalshi KXMAYORNYCPARTY-25-D is "a Democrat
    wins NYC mayor"; Polymarket is "Mamdani wins". These are only equivalent once
    Mamdani won the Democratic primary (Cuomo conceded the night of 2025-06-24;
    the two YES prices snapped together by ~2025-06-25 01:22 UTC). Before that the
    "arbitrage" compares different questions, so we start at START_TS.
  - Each row = ONE Polymarket trade, matched to the NEAREST Kalshi trade in time
    within a 5-minute window (the trade may be before OR after it). This is an
    as-of/nearest join: many poly trades can share one kalshi trade.
  - poly_side = the poly trade's outcome (YES/NO); kalshi_side = the opposite.
  - Emit the row only if poly_price[poly_side] + kalshi_price[kalshi_side] < 1,
    i.e. an executable arbitrage existed at that poly fill.
  - arb_size = the poly trade's quantity (the size actually transacted on the
    arbitrage side).

Columns: poly_time, kalshi_time, poly_yes, poly_no, kalshi_yes, kalshi_no,
         arb_size, poly_side, kalshi_side

Run:
    python src/build_arbitrage.py
"""

import bisect
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MERGED = ROOT / "data" / "clean" / "merged_trades_mamdani_by_timestamp.csv"
OUT = ROOT / "data" / "clean" / "arbitrage.csv"

WINDOW_S = 300  # 5 minutes
# Mamdani became the Democratic nominee (= Kalshi's "Democrat wins" event) when
# Cuomo conceded the primary the night of 2025-06-24; prices fully converged by
# 2025-06-25 01:22 UTC. Start just after, so the two venues price the same event.
import datetime as _dt
START_TS = int(_dt.datetime(2025, 6, 25, 2, 0, tzinfo=_dt.timezone.utc).timestamp())


def rnd(x: float) -> float:
    return round(x, 6)


def main():
    poly, kal = [], []
    for r in csv.DictReader(MERGED.open(encoding="utf-8")):
        ts = int(r["unix_ts"])
        yes = float(r["yes_price"])
        if r["venue"] == "polymarket":
            poly.append((ts, yes, r["outcome"].upper(), float(r["quantity"])))
        elif r["venue"] == "kalshi":
            kal.append((ts, yes))
    poly.sort(key=lambda x: x[0])
    kal.sort(key=lambda x: x[0])

    # overlap window: later start -> earlier end, floored at same-event date
    start = max(poly[0][0], kal[0][0], START_TS)
    end = min(poly[-1][0], kal[-1][0])

    kal_ts = [k[0] for k in kal]

    def nearest_kalshi(ts):
        """Nearest kalshi trade in time; None if none within WINDOW_S."""
        i = bisect.bisect_left(kal_ts, ts)
        best = None
        for j in (i - 1, i):
            if 0 <= j < len(kal):
                d = abs(kal[j][0] - ts)
                if d <= WINDOW_S and (best is None or d < best[0]):
                    best = (d, kal[j])
        return best[1] if best else None

    rows = []
    for ts, p_yes, side, qty in poly:
        if ts < start or ts > end:
            continue
        k = nearest_kalshi(ts)
        if k is None:
            continue
        k_ts, k_yes = k
        p_no, k_no = 1.0 - p_yes, 1.0 - k_yes
        # price paid on each leg: poly on its own side, kalshi on the opposite
        if side == "YES":
            kside, cost = "NO", p_yes + k_no
        else:  # poly NO
            kside, cost = "YES", p_no + k_yes
        if cost >= 1.0:
            continue  # no arbitrage at this fill
        rows.append({
            "poly_time": ts,
            "kalshi_time": k_ts,
            "poly_yes": rnd(p_yes),
            "poly_no": rnd(p_no),
            "kalshi_yes": rnd(k_yes),
            "kalshi_no": rnd(k_no),
            "arb_size": qty,
            "poly_side": side,
            "kalshi_side": kside,
        })

    rows.sort(key=lambda r: (r["poly_time"], r["kalshi_time"]))
    fields = ["poly_time", "kalshi_time", "poly_yes", "poly_no", "kalshi_yes",
              "kalshi_no", "arb_size", "poly_side", "kalshi_side"]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    profit = sum((1.0 - (r["poly_yes"] + r["kalshi_no"] if r["poly_side"] == "YES"
                         else r["poly_no"] + r["kalshi_yes"])) * r["arb_size"] for r in rows)
    print(f"Wrote {len(rows):,} arbitrage rows -> {OUT}")
    if rows:
        import datetime as dt
        print(f"  window: {dt.datetime.utcfromtimestamp(start)} -> {dt.datetime.utcfromtimestamp(end)} UTC")
        print(f"  total arb_size (poly shares): {sum(r['arb_size'] for r in rows):,.0f}")
        print(f"  theoretical locked profit: ${profit:,.0f}")


if __name__ == "__main__":
    main()
