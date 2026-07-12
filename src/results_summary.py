"""
Per-venue results summary from an event's arbitrage_final.csv, using the
two-variable cash+position model (see portfolio.py).

Each venue is one (y, z) pair — cash and net signed YES-equivalent
contracts; NO purchases are folded in as YES shorts at ingestion, so there
is no separate YES/NO holding anywhere. The event's resolution supplies
the terminal YES price (YES -> 1.0, NO -> 0.0):
  market_value = y + z * yes_price
  net          = poly_market_value + kalshi_market_value

Per venue the report shows:
  tokens   — z, the net YES-equivalent position (negative = short YES)
  expense  — total cash paid for contracts (actual outflow)
  payback  — settlement receipts (winning contracts pay $1)
  earnings — payback - expense (equals y + z*settle)
  net      — y + z*settle from the model
Then the overall Net. The hedge check (|z_poly| vs |z_kalshi|) runs as a
stdout diagnostic. Writes events/<slug>/results/results.txt.

Run:
    python src/results_summary.py mamdani-nyc-mayor
"""
import csv
import sys

from eventlib import load_event
from portfolio import Portfolio


def main() -> None:
    ev = load_event(sys.argv[1] if len(sys.argv) > 1 else None)
    res = (ev.resolution or "YES").upper()
    settle_yes = 1.0 if res == "YES" else 0.0

    pf = Portfolio()
    # cash-flow counters for reporting (valuation itself is just (y, z))
    flow = {"poly": {"expense": 0.0, "payback": 0.0},
            "kalshi": {"expense": 0.0, "payback": 0.0}}

    for r in csv.DictReader(ev.arb_final_csv.open(encoding="utf-8")):
        sz = int(r["arb_size"])
        for venue, pos, side_col, yes_col, no_col in (
                ("poly", pf.poly, "poly_side", "poly_yes", "poly_no"),
                ("kalshi", pf.kalshi, "kalshi_side", "kalshi_yes", "kalshi_no")):
            if r[side_col] == "YES":
                price = float(r[yes_col])
                pos.buy_yes(sz, price)
                if res == "YES":
                    flow[venue]["payback"] += sz
            else:
                price = float(r[no_col])
                pos.buy_no(sz, price)
                if res == "NO":
                    flow[venue]["payback"] += sz
            flow[venue]["expense"] += sz * price

    if not pf.is_hedged:  # diagnostic only, never expected on matched output
        print(f"WARNING: unmatched exposure |z_poly| - |z_kalshi| = {pf.hedge_delta}")

    lines = []
    for label, venue, pos in (("Poly", "poly", pf.poly),
                              ("Kalshi", "kalshi", pf.kalshi)):
        f = flow[venue]
        lines += [
            f"{label} tokens: {pos.z}",
            f"{label} expense: {f['expense']}",
            f"{label} payback: {f['payback']}",
            f"{label} earnings: {f['payback'] - f['expense']}",
            f"{label} net: {pos.value(settle_yes)}",
            "",
        ]
    lines.append(f"Net: {pf.net_value(settle_yes, settle_yes)}")

    out = ev.results_dir / "results.txt"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"\n-> {out}")


if __name__ == "__main__":
    main()
