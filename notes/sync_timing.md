# When do the two venues sync? (popvote-2024)

Reproduce with: `python src/analyze_sync.py popvote-2024`

Overlap window: 2024-10-07 .. 2024-11-11 (Kalshi didn't list this contract until
Oct 7, 2024).

## Headline

**There is no single sync time — it is regime-dependent, and the two regimes differ
by more than an order of magnitude.**

| regime | basis half-life | ~95% re-sync |
|---|---|---|
| Election night (active / news) | **~97 seconds** | ~7 min |
| Pre-election (quiet) | **~36 minutes** | ~2.6 hours |
| Full overlap (blended) | ~22 minutes | ~94 min |

Measured as the AR(1) decay of the spread `b_t = kalshi_yes − poly_yes`
(60s sampling): `b_t = a + φ·b_{t−1}`, half-life = `−ln2 / ln φ`.
Election night φ = 0.652; pre-election φ = 0.981.

The mean spread itself is stable at **−0.034** (Polymarket ~3.4c above Kalshi) with
sd 0.018 in *both* regimes — the *level* of the basis doesn't change, only the
*speed at which deviations from it decay*.

## Neither venue leads

Cross-correlation `corr(dKalshi_t, dPoly_{t−k})`:
- On 5-min changes: peak at **lag 0** (corr 0.134).
- On 15-min changes: peak at lag +180s (corr 0.359) vs 0.345 at lag 0 — a
  Polymarket lead so slight, on a curve so flat and symmetric, that it is not a
  usable signal.

They drift together; one does not chase the other.

## They are not coupled at all at high frequency (Epps effect)

Correlation of price changes vs sampling interval:

| bin | 5s | 30s | 60s | 120s | 300s | 600s | 900s | 1800s |
|---|---|---|---|---|---|---|---|---|
| corr | −0.02 | 0.04 | 0.00 | 0.08 | **0.21** | 0.35 | 0.48 | **0.66** |

This is the classic **Epps effect** (non-synchronous trading + bid-ask bounce), not
evidence the markets are unrelated. Two traps it sets:

1. **Do not measure lead-lag at 1s resolution.** It returns ~0.00 correlation and a
   meaningless "peak" at an arbitrary lag. An earlier version of this analysis did
   exactly that and produced pure noise.
2. **Do not correlate price *levels*.** Both series trend from 0.26 → 0.99 and share
   a persistent basis, so levels correlate ~1.0 for reasons that carry no
   information. Always work in first differences.

## Consequence for `window_seconds` (currently 300)

`merge_for_arb.py` treats a Kalshi trade and a Polymarket trade within
`window_seconds` as **simultaneous**. Two problems at 300s:

1. **300s is ~3.1× the active-regime half-life (97s).** A pair at the edge of the
   window is one where the spread has already had ~3 half-lives to re-converge on
   its own. Calling those two trades "simultaneous" is not defensible in exactly
   the regime (election night) where the volume — and the apparent arbitrage — is.

2. **Intra-window drift swamps the edge.** Election-night `|Δ yes_price|`:

   | horizon | Kalshi p90 / p99 | Polymarket p90 / p99 |
   |---|---|---|
   | 60s | 2.0c / 5.0c | 0.8c / 4.3c |
   | 300s | 3.0c / 6.0c | 1.7c / 7.2c |

   The mean basis is only ~3.4c. At a 300s window a single leg can move 3–6c, so a
   "locked-in" arbitrage of a few cents is **indistinguishable from price drift
   between the two legs** — you would be reporting an edge you could never have
   executed.

**Recommendation:** use **≤60s** (drift p90 ≤ 2c, comfortably inside the 97s
half-life). Better still, report arbitrage results as a *sensitivity curve* over
window ∈ {10, 30, 60, 300}s — if the claimed profit only appears at 300s, it is an
artifact of the window, not an arbitrage.

Note this cuts both ways: a shorter window is *conservative* (fewer, better-founded
matches). A longer one manufactures opportunities.
