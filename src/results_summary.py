"""
Simple per-venue results summary from an event's arbitrage_final.csv.

For each venue: YES/NO tokens matched, cash deployed, and settled earnings under
the event's resolution (winning-outcome tokens pay $1), plus the net. Writes
events/<slug>/results/results.txt.

Run:
    python src/results_summary.py mamdani-nyc-mayor
"""
import csv
import sys

from eventlib import load_event


def main() -> None:
    ev = load_event(sys.argv[1] if len(sys.argv) > 1 else None)
    res = (ev.resolution or "YES").upper()

    p_yes = p_no = k_yes = k_no = 0.0
    p_cash = k_cash = 0.0
    for r in csv.DictReader(ev.arb_final_csv.open(encoding="utf-8")):
        sz = int(r["arb_size"])
        if r["poly_side"] == "YES":
            p_yes += sz
            p_cash += float(r["poly_yes"]) * sz
        else:
            p_no += sz
            p_cash += float(r["poly_no"]) * sz
        if r["kalshi_side"] == "YES":
            k_yes += sz
            k_cash += float(r["kalshi_yes"]) * sz
        else:
            k_no += sz
            k_cash += float(r["kalshi_no"]) * sz

    # settled: the resolved outcome's tokens each pay $1
    p_earn = (p_yes if res == "YES" else p_no) - p_cash
    k_earn = (k_yes if res == "YES" else k_no) - k_cash
    net = p_earn + k_earn

    lines = [
        f"Poly yes tokens: {p_yes}",
        f"Poly no tokens: {p_no}",
        f"Poly cash: {p_cash}",
        f"Poly earnings: {p_earn}",
        "",
        f"Kalshi yes tokens: {k_yes}",
        f"Kalshi no tokens: {k_no}",
        f"Kalshi cash: {k_cash}",
        f"Kalshi earnings: {k_earn}",
        "",
        f"Net: {net}",
    ]
    out = ev.results_dir / "results.txt"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"\n-> {out}")


if __name__ == "__main__":
    main()
