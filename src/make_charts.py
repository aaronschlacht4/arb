"""
Build an event's charts: results/charts.html (interactive, self-contained) plus
prices.png / positions.png / locked_in.png (static exports of the same three).

Nothing in the repo generated these before — the earlier events' charts were made
outside it, so they could not be reproduced or extended to a new event. This
closes that gap.

THE THREE CHARTS (all driven by the SAME portfolio replay, see portfolio.py):

  prices     — each venue's implied P(event) (`yes_price`) over time. The gap
               between the two lines IS the arbitrage signal.
  positions  — net signed YES-equivalent position (z) per venue. A correct hedge
               is symmetric: the two lines mirror each other about zero, because
               every match goes long one venue and short the other.
  locked_in  — cumulative y_poly + y_kalshi, the PRICE-INVARIANT cash locked in.
               This is the honest P&L line: it does not depend on where prices go
               next, which is exactly what makes an arbitrage an arbitrage.

The HTML reuses src/charts_template.html (the original hand-built page with its
DATA/ORDER blocks swapped for placeholders), so the design is preserved exactly
and only the data changes.

Series are down-sampled to ~700 points via a step-function lookup (value as of
each grid time), which is what the charts need and keeps a 742k-match event light.

Run:
    python src/make_charts.py presidential-2024
    python src/make_charts.py --all
"""

import argparse
import csv
import json
import sys
from pathlib import Path

import numpy as np

import matplotlib
matplotlib.use("Agg")            # headless: write files, never open a window
import matplotlib.pyplot as plt  # noqa: E402

from eventlib import EVENTS_DIR, ROOT, load_event  # noqa: E402
from portfolio import Portfolio  # noqa: E402

TEMPLATE = ROOT / "src" / "charts_template.html"
N_POINTS = 700

# Same palette as the HTML template, so the PNGs and the page match.
C_POLY, C_KALSHI, C_LOCKED = "#2a78d6", "#1baf7a", "#4a3aa7"


def step_sample(ts: np.ndarray, vals: np.ndarray, grid: np.ndarray):
    """Value as of each grid time (last observation at or before it).

    searchsorted gives the step-function lookup in one pass — important for
    presidential-2024, where a naive per-point scan over 2.7M trades would crawl.
    """
    idx = np.searchsorted(ts, grid, side="right") - 1
    out = np.full(grid.size, np.nan)
    ok = idx >= 0
    out[ok] = vals[idx[ok]]
    return out


def replay(ev):
    """Walk arbitrage_final.csv through the portfolio model, recording the path.

    A match is booked at max(poly_time, kalshi_time): the position only exists
    once BOTH legs have filled.
    """
    pf = Portfolio()
    t, zp, zk, lock = [], [], [], []
    n = 0
    with ev.arb_final_csv.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            sz = int(r["arb_size"])
            if r["poly_side"] == "YES":
                pf.poly.buy_yes(sz, float(r["poly_yes"]))
            else:
                pf.poly.buy_no(sz, float(r["poly_no"]))
            if r["kalshi_side"] == "YES":
                pf.kalshi.buy_yes(sz, float(r["kalshi_yes"]))
            else:
                pf.kalshi.buy_no(sz, float(r["kalshi_no"]))
            t.append(max(int(r["poly_time"]), int(r["kalshi_time"])))
            zp.append(pf.poly.z)
            zk.append(pf.kalshi.z)
            lock.append(pf.locked_in_value)
            n += 1
    if not n:
        raise SystemExit(f"No matches in {ev.arb_final_csv}")
    # Matches are booked at max(leg times), which need not be monotone — sort so
    # the step lookup is valid.
    o = np.argsort(np.asarray(t, dtype=np.int64), kind="stable")
    return (np.asarray(t, dtype=np.int64)[o], np.asarray(zp)[o],
            np.asarray(zk)[o], np.asarray(lock)[o], n, pf)


def price_series(path: Path):
    """Full-market price path from a clean tape."""
    ts, px = [], []
    with path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            ts.append(int(r["unix_ts"]))
            px.append(float(r["yes_price"]))
    ts = np.asarray(ts, dtype=np.int64)
    px = np.asarray(px)
    o = np.argsort(ts, kind="stable")
    return ts[o], px[o]


def price_series_from_arb(ev, venue: str):
    """Fallback price path, taken from arbitrage_final.csv itself.

    Used when an event's clean tapes aren't present (the mamdani events ship
    their results/ in git but their raw/ and clean/ are gitignored). This is the
    MATCHED SUBSET of trades, not the whole market — fine for the price line's
    shape, but it is not the full tape. Re-fetch the raw data for an exact chart.
    """
    tcol, pcol = f"{venue}_time", f"{venue}_yes"
    ts, px = [], []
    with ev.arb_final_csv.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            ts.append(int(r[tcol]))
            px.append(float(r[pcol]))
    ts = np.asarray(ts, dtype=np.int64)
    px = np.asarray(px)
    o = np.argsort(ts, kind="stable")
    return ts[o], px[o]


def png(path: Path, series, title: str, ylabel: str, zero_line: bool = False):
    fig, ax = plt.subplots(figsize=(11, 4.2), dpi=140)
    for name, color, x, y in series:
        ax.step(x, y, where="post", lw=1.4, color=color, label=name)
    if zero_line:
        ax.axhline(0, lw=0.9, color="#c3c2b7", zorder=0)
    ax.set_title(title, fontsize=12, loc="left", pad=10)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.grid(alpha=0.18, lw=0.6)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, fontsize=9, loc="best")
    ax.tick_params(labelsize=8)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def build(slug: str, allow_subset: bool = False) -> dict:
    ev = load_event(slug)
    if not ev.arb_final_csv.exists():
        print(f"  {slug}: no arbitrage_final.csv — run run_arbitrage.py first, skipping")
        return {}

    t, zp, zk, lock, n_match, pf = replay(ev)
    lo, hi = int(t[0]), int(t[-1])
    grid = np.linspace(lo, hi, N_POINTS).astype(np.int64)

    # Prices come from the CLEAN TAPES where we have them — we want the whole
    # market, not just the trades that happened to arb. Fall back to the matched
    # subset in arbitrage_final.csv for events whose tapes aren't in this clone.
    poly_clean = (ev.poly_rpc_clean_csv if ev.poly_rpc_clean_csv.exists()
                  else ev.poly_subgraph_clean_csv)
    if ev.kalshi_clean_csv.exists() and poly_clean.exists():
        kt, kp = price_series(ev.kalshi_clean_csv)
        pt, pp = price_series(poly_clean)
        px_src = "clean tapes"
    elif allow_subset:
        kt, kp = price_series_from_arb(ev, "kalshi")
        pt, pp = price_series_from_arb(ev, "poly")
        px_src = "MATCHED SUBSET (clean tapes absent — prices are approximate)"
    else:
        # Refuse rather than quietly write WORSE charts over good ones. The
        # matched-subset fallback only sees trades that arbed, so its price line
        # is not the market's. Re-fetch the tapes, or pass --allow-subset if you
        # accept the approximation.
        print(f"  {slug}: clean tapes absent — refusing to overwrite charts with "
              f"matched-subset prices. Re-fetch, or pass --allow-subset.")
        return {}
    k_grid = step_sample(kt, kp, grid)
    p_grid = step_sample(pt, pp, grid)

    zp_g = step_sample(t, zp, grid)
    zk_g = step_sample(t, zk, grid)
    lk_g = step_sample(t, lock, grid)

    def pairs(g, v, r=None):
        return [[int(a), (round(float(b), r) if r is not None else float(b))]
                for a, b in zip(g, v) if not np.isnan(b)]

    data = {
        "window": [lo, hi],
        "prices": {"poly": pairs(grid, p_grid, 3), "kalshi": pairs(grid, k_grid, 3)},
        "positions": [[int(a), float(b), float(c)]
                      for a, b, c in zip(grid, zp_g, zk_g) if not np.isnan(b)],
        "locked": pairs(grid, lk_g),
        "matches": n_match,
        "final_z": [float(pf.poly.z), float(pf.kalshi.z)],
        "locked_final": float(pf.locked_in_value),
        "title": ev.cfg.get("chart_title", ev.slug),
        "question": ev.name,
    }

    # --- static PNGs ---
    import matplotlib.dates as mdates  # noqa: F401
    gd = np.array([np.datetime64(int(x), "s") for x in grid])
    res = ev.results_dir
    png(res / "prices.png",
        [("Polymarket", C_POLY, gd, p_grid), ("Kalshi", C_KALSHI, gd, k_grid)],
        f"{data['title']} — YES price by venue", "implied P(event)")
    png(res / "positions.png",
        [("Poly", C_POLY, gd, zp_g), ("Kalshi", C_KALSHI, gd, zk_g)],
        f"{data['title']} — net YES-equivalent position", "contracts",
        zero_line=True)
    png(res / "locked_in.png",
        [("Locked-in", C_LOCKED, gd, lk_g)],
        f"{data['title']} — locked-in value (price-invariant)", "USD",
        zero_line=True)

    # --- interactive page (reuses the original design verbatim) ---
    tpl = TEMPLATE.read_text(encoding="utf-8")
    html = (tpl.replace("__DATA__", json.dumps({slug: data}, separators=(",", ":")))
               .replace("__ORDER__", json.dumps([slug])))
    (res / "charts.html").write_text(html, encoding="utf-8")

    print(f"  {slug}: {n_match:,} matches | locked-in ${pf.locked_in_value:,.0f} "
          f"| prices from {px_src}")
    return data


def main() -> None:
    ap = argparse.ArgumentParser(description="Build charts for an event.")
    ap.add_argument("event", nargs="?", help="Event slug")
    ap.add_argument("--all", action="store_true", help="every event with results")
    ap.add_argument("--allow-subset", action="store_true",
                    help="if the clean tapes are missing, draw prices from the "
                         "matched subset in arbitrage_final.csv (approximate)")
    args = ap.parse_args()

    slugs = ([p.parent.name for p in sorted(EVENTS_DIR.glob("*/event.json"))]
             if args.all else [args.event or None])
    for s in slugs:
        if s is None:
            raise SystemExit("Pass an event slug or --all")
        build(s, allow_subset=args.allow_subset)


if __name__ == "__main__":
    main()
