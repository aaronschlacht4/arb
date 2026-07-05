"""
Settlement backtest: "buy" every arbitrage in the log and compute the realized
profit on BOTH venues when the event closed.

Outcome: the Mamdani / "a Democrat wins NYC mayor" event resolved YES (Mamdani won
the 2025-11-04 election; final trades printed ~0.99 YES on both venues). So YES
shares pay $1 and NO shares pay $0 at settlement.

For each arbitrage row we buy `arb_size` shares on each leg:
  - the venue whose poly_side/kalshi_side == YES: bought YES at that venue's YES
    price -> pays $1  (this leg WINS)
  - the venue whose side == NO: bought NO at that venue's NO price -> pays $0
    (this leg LOSES its whole stake)
  net per share = 1 - (yes_leg_price + no_leg_price) = the edge, regardless of
  outcome (that's what makes it an arbitrage). But the PER-VENUE P&L is lopsided:
  one venue's position wins, the other's expires worthless.

Input : data/clean/arbitrage.csv  (the per-fill arbitrage log)
Output: data/clean/arbitrage_settlement.csv  + printed portfolio summary

Run:
    python src/build_arbitrage_settlement.py
"""

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IN = ROOT / "data" / "clean" / "arbitrage.csv"
OUT = ROOT / "data" / "clean" / "arbitrage_settlement.csv"

OUTCOME = "YES"  # Mamdani / Democrat won


def leg_price(r, venue, side):
    return float(r[f"{venue}_{side.lower()}"])


def main():
    out = []
    tot_cost = tot_payout = poly_net = kalshi_net = 0.0
    for r in csv.DictReader(IN.open(encoding="utf-8")):
        size = float(r["arb_size"])
        pside, kside = r["poly_side"], r["kalshi_side"]
        p_buy = leg_price(r, "poly", pside)      # price paid per share on Polymarket
        k_buy = leg_price(r, "kalshi", kside)    # price paid per share on Kalshi
        p_cost, k_cost = size * p_buy, size * k_buy
        # payout: a leg pays $1/share iff its side == the winning outcome
        p_payout = size if pside == OUTCOME else 0.0
        k_payout = size if kside == OUTCOME else 0.0
        p_profit = p_payout - p_cost
        k_profit = k_payout - k_cost
        total_profit = p_profit + k_profit

        tot_cost += p_cost + k_cost
        tot_payout += p_payout + k_payout
        poly_net += p_profit
        kalshi_net += k_profit

        out.append({
            "poly_time": r["poly_time"], "kalshi_time": r["kalshi_time"],
            "poly_side": pside, "kalshi_side": kside, "arb_size": size,
            "poly_buy_price": round(p_buy, 6), "kalshi_buy_price": round(k_buy, 6),
            "poly_cost": round(p_cost, 4), "kalshi_cost": round(k_cost, 4),
            "total_cost": round(p_cost + k_cost, 4),
            "outcome": OUTCOME,
            "poly_payout": round(p_payout, 4), "kalshi_payout": round(k_payout, 4),
            "poly_profit": round(p_profit, 4), "kalshi_profit": round(k_profit, 4),
            "total_profit": round(total_profit, 4),
        })

    fields = ["poly_time", "kalshi_time", "poly_side", "kalshi_side", "arb_size",
              "poly_buy_price", "kalshi_buy_price", "poly_cost", "kalshi_cost",
              "total_cost", "outcome", "poly_payout", "kalshi_payout",
              "poly_profit", "kalshi_profit", "total_profit"]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(out)

    total_profit = tot_payout - tot_cost
    print(f"Wrote {len(out):,} bought-and-settled arbitrage rows -> {OUT}")
    print(f"\nPortfolio (event resolved {OUTCOME}):")
    print(f"  total invested (both legs) : ${tot_cost:,.0f}")
    print(f"  total returned at close    : ${tot_payout:,.0f}")
    print(f"  net profit                 : ${total_profit:,.0f}  ({100*total_profit/tot_cost:.2f}% on cost)")
    print(f"\n  Per-venue realized P&L (one side always wins, the other expires worthless):")
    print(f"    Polymarket : ${poly_net:,.0f}")
    print(f"    Kalshi     : ${kalshi_net:,.0f}")
    print(f"    combined   : ${poly_net + kalshi_net:,.0f}")
    winners = sum(1 for r in out if r["total_profit"] > 0)
    print(f"\n  rows with positive locked profit: {winners:,}/{len(out):,} "
          f"(every hedged pair >0 by construction)")


if __name__ == "__main__":
    main()
