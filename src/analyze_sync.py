"""
Measure HOW FAST the two venues synchronise, and use that to test the
`window_seconds` the arbitrage matcher relies on.

WHY THIS MATTERS
    merge_for_arb.py pairs a Kalshi trade with a Polymarket trade when they fall
    within `window_seconds` (event.json; 300s) and treats the pair as
    SIMULTANEOUS. That is only legitimate if (a) the venues have actually
    re-priced each other within that window, and (b) neither price can drift far
    inside it. If either fails, the matcher pairs trades that were never
    contemporaneous and reports arbitrage that was never executable.

WHAT WE MEASURE  (three complementary things — one number would mislead)

  1. EPPS CURVE — correlation of price changes vs sampling interval.
     Cross-market correlation collapses at high frequency (non-synchronous
     trading + bid-ask bounce). Plotting corr against the interval shows the
     timescale at which the venues are actually coupled at all.
     NOTE: measuring this at 1s resolution returns ~0.00 and is NOT evidence the
     venues are unrelated — it is the Epps effect. Do not read it as a finding.

  2. LEAD-LAG — xcorr(k) = corr(dKalshi_t, dPoly_{t-k}).
     k > 0 => Polymarket leads; k < 0 => Kalshi leads. Run at a horizon where
     signal exists (see 1), otherwise you are correlating noise.

  3. BASIS HALF-LIFE — the operationally meaningful one.
     The spread b_t = kalshi_yes - poly_yes carries a persistent level (basis).
     Fit AR(1) on the spread: b_t = a + phi*b_{t-1}. The half-life
     -ln2/ln(phi) is how long a DISLOCATION takes to decay by half — i.e. how
     long the two venues stay out of line. This is the real "sync time".

     Paired with the DRIFT table (how far each venue can move inside a candidate
     window), it tells you whether `window_seconds` is defensible: the window
     must be short relative to the half-life, and short enough that intra-window
     drift is small relative to the arbitrage edge you are trying to detect.

Run:
    python src/analyze_sync.py popvote-2024
"""

import argparse

import numpy as np
import pandas as pd

from eventlib import load_event

# Election night: first results land ~2024-11-06 00:00 UTC (7pm ET Nov 5).
SHOCK_START = 1730851200
SHOCK_END = SHOCK_START + 86_400

EPPS_BINS = (5, 10, 30, 60, 120, 300, 600, 900, 1800)
DRIFT_HORIZONS = (10, 30, 60, 300, 600)
LEAD_LAG_HORIZONS = (300, 900)


def second_grid(df: pd.DataFrame, lo: int, hi: int) -> pd.Series:
    """1-second last-trade yes_price, forward-filled (a quiet second still has a
    price — the last one traded)."""
    s = df.groupby("unix_ts")["yes_price"].last()
    return s.reindex(np.arange(lo, hi + 1)).ffill()


def epps_curve(k: pd.DataFrame, p: pd.DataFrame) -> None:
    """Correlation of VWAP price changes as a function of sampling interval."""
    print("\n1) EPPS CURVE — do the venues co-move at all, and on what timescale?")
    print("   (VWAP per bin, only bins where BOTH venues traded, adjacent bins)")
    print(f"   {'bin':>7}   {'n':>7}   corr(dKalshi, dPoly)")

    def vwap(df, b):
        g = df.assign(bin=(df.unix_ts // b) * b,
                      pq=df.yes_price * df.quantity).groupby("bin")
        return g.pq.sum() / g.quantity.sum()

    for b in EPPS_BINS:
        kb, pb = vwap(k, b), vwap(p, b)
        idx = kb.index.intersection(pb.index)
        s = pd.DataFrame({"k": kb.reindex(idx), "p": pb.reindex(idx)}).sort_index()
        d = s.diff()
        adjacent = pd.Series(s.index).diff().values == b  # no gap between bins
        dk, dp = d.k.values[adjacent], d.p.values[adjacent]
        m = ~(np.isnan(dk) | np.isnan(dp))
        dk, dp = dk[m], dp[m]
        if dk.size < 50 or dk.std() == 0 or dp.std() == 0:
            print(f"   {b:>6}s   {dk.size:>7,}   (too few)")
            continue
        c = np.corrcoef(dk, dp)[0, 1]
        print(f"   {b:>6}s   {dk.size:>7,}   {c:+.4f}  {'#' * int(max(0, c) * 60)}")


def lead_lag(K: np.ndarray, P: np.ndarray, max_lag: int = 1800,
             step: int = 60) -> None:
    print("\n2) LEAD-LAG — who moves first?")
    for h in LEAD_LAG_HORIZONS:
        rK, rP = K[h:] - K[:-h], P[h:] - P[:-h]
        lags = np.arange(-max_lag, max_lag + 1, step)
        c = []
        for L in lags:
            if L > 0:
                a, b = rK[L:], rP[:-L]
            elif L < 0:
                a, b = rK[:L], rP[-L:]
            else:
                a, b = rK, rP
            c.append(np.corrcoef(a, b)[0, 1])
        c = np.array(c)
        best = int(np.argmax(c))
        pk = int(lags[best])
        who = ("Polymarket leads" if pk > 0 else
               "Kalshi leads" if pk < 0 else "simultaneous")
        print(f"\n   {h}s-horizon changes: peak corr {c[best]:.4f} at lag "
              f"{pk:+d}s  ({who})")
        print(f"   corr at lag 0 = {c[lags == 0][0]:.4f}   "
              f"[flat, symmetric curve => no venue reliably leads]")
        for L in (-900, -300, -120, -60, 0, 60, 120, 300, 900):
            v = c[lags == L][0]
            print(f"     {L:+6d}s  {v:+.4f}  {'#' * int(max(0, v) * 50)}")


def basis_half_life(basis: pd.Series, label: str) -> float:
    """AR(1) half-life of the cross-venue spread, sampled at 60s."""
    b = basis.iloc[::60].to_numpy()
    b = b[~np.isnan(b)]
    if b.size < 50:
        return float("nan")
    phi = np.polyfit(b[:-1], b[1:], 1)[0]
    hl = -np.log(2) / np.log(phi) if 0 < phi < 1 else float("nan")
    print(f"   {label:16} mean {b.mean():+.4f}  sd {b.std():.4f}  "
          f"phi {phi:.4f}  half-life "
          + (f"{hl:6.1f} min   (95% resync ~{4.32 * hl:.0f} min)"
             if not np.isnan(hl) else "  n/a (non-stationary)"))
    return hl


def drift_table(S: pd.Series, name: str) -> None:
    a = S.to_numpy()
    print(f"   {name}")
    for h in DRIFT_HORIZONS:
        d = np.abs(a[h:] - a[:-h]) * 100
        print(f"     {h:>4}s: median {np.median(d):5.2f}c   "
              f"p90 {np.percentile(d, 90):5.2f}c   "
              f"p99 {np.percentile(d, 99):6.2f}c")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Measure cross-venue synchronisation timing.")
    ap.add_argument("event", nargs="?", help="Event slug, e.g. popvote-2024")
    args = ap.parse_args()

    ev = load_event(args.event)
    cols = ["unix_ts", "yes_price", "quantity"]
    k = pd.read_csv(ev.kalshi_clean_csv, usecols=cols)
    p = pd.read_csv(ev.poly_subgraph_clean_csv, usecols=cols)

    lo = max(int(k.unix_ts.min()), int(p.unix_ts.min()))
    hi = min(int(k.unix_ts.max()), int(p.unix_ts.max()))
    k = k[(k.unix_ts >= lo) & (k.unix_ts <= hi)]
    p = p[(p.unix_ts >= lo) & (p.unix_ts <= hi)]

    print(f"event   : {ev.slug}")
    print(f"overlap : {pd.to_datetime(lo, unit='s')} .. "
          f"{pd.to_datetime(hi, unit='s')} UTC")
    print(f"testing : window_seconds = {ev.window_seconds} (from event.json)")

    epps_curve(k, p)

    Kg, Pg = second_grid(k, lo, hi), second_grid(p, lo, hi)
    ok = Kg.notna() & Pg.notna()
    Kg, Pg = Kg[ok], Pg[ok]
    lead_lag(Kg.to_numpy(), Pg.to_numpy())

    basis = Kg - Pg
    print("\n3) BASIS HALF-LIFE — how long do the venues stay dislocated?")
    print("   spread = kalshi_yes - poly_yes;  AR(1) at 60s sampling")
    shock = (basis.index >= SHOCK_START) & (basis.index < SHOCK_END)
    hl_active = np.nan
    for label, sel in (("full overlap", slice(None)),
                       ("pre-election", basis.index < SHOCK_START),
                       ("election night", shock)):
        hl = basis_half_life(basis[sel] if not isinstance(sel, slice) else basis,
                             label)
        if label == "election night":
            hl_active = hl

    print("\n4) INTRA-WINDOW DRIFT — how far can a price run inside the window?")
    print("   |change in yes_price| over the horizon, ELECTION NIGHT (worst case)")
    drift_table(Kg[shock], "kalshi")
    drift_table(Pg[shock], "polymarket")

    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)
    if not np.isnan(hl_active):
        print(f"  Dislocations decay with a half-life of ~{hl_active * 60:.0f}s when the")
        print(f"  market is ACTIVE. window_seconds={ev.window_seconds} is "
              f"{ev.window_seconds / (hl_active * 60):.1f}x that half-life, so a")
        print("  'simultaneous' pair at the edge of the window is one in which the")
        print("  spread has had time to substantially re-converge on its own.")
    print("  Compare the p90/p99 drift above against the arbitrage edge you intend")
    print("  to claim: if the edge is ~3c and a leg can move 3-6c inside the window,")
    print("  the 'arbitrage' is indistinguishable from drift between the two legs.")


if __name__ == "__main__":
    main()
