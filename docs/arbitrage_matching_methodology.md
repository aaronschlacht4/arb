# Cross-Venue Arbitrage Matching — Methodology

*How Wendy's `arbitrage.py` detects hypothetical Polymarket ⇄ Kalshi arbitrage on
the "Will Zohran Mamdani win the Democratic Primary?" market, and how it produces
`arbitrage_final.csv` (the arb pairs) and `matched_trades.csv` (the per-trade log).*

This document is a detailed walkthrough of her method — every step, why it's
there, and worked examples on the real data. Our reproduction lives in
[`src/build_arbitrage_matched_trades.py`](../src/build_arbitrage_matched_trades.py),
which is a faithful port verified to reproduce all 122,635 rows of
`matched_trades.csv` exactly.

---

## 1. The goal, in one sentence

> Pretend you are a high-frequency trading bot watching both venues. Every time
> the same event is priced differently on Polymarket and Kalshi within a 5-minute
> window, "execute" the risk-free trade and record it.

This is **hypothetical** arbitrage: it asks *"how much mispricing existed and was
in principle capturable,"* not *"how much a real trader definitely could have
filled."* That framing drives several of the modeling choices below.

---

## 2. Why any price gap is an arbitrage

Each market sells two complementary contracts:

- **YES** pays $1 if Mamdani wins, $0 otherwise.
- **NO** pays $1 if he loses, $0 otherwise.

Exactly one happens, so **YES + NO always settles to $1**. Therefore the two
prices must sum to $1: if YES = 0.71, NO = 0.29 by definition.

Now suppose the *same* event has different YES prices on the two venues:

| Venue | YES | NO |
|---|---|---|
| Polymarket | 0.715 | 0.285 |
| Kalshi | 0.710 | 0.290 |

Buy the **cheaper side on each venue so you cover both outcomes**:

- Buy Polymarket **NO** @ 0.285
- Buy Kalshi **YES** @ 0.710
- **Total cost = 0.995** for a position that pays $1 no matter what → **0.5¢
  locked profit per contract.**

The profit is exactly the price gap (`0.715 − 0.710 = 0.005`). So the arb test is
simply: **do the two YES prices differ?** Her code writes this as

```python
abs(poly_yes - kalshi_yes) >= EPS      # EPS = 1e-4
```

Any gap ≥ 0.0001 is an arbitrage. (This is mathematically identical to
"combined cost < $1"; it's just phrased as a price difference.)

**Side assignment** follows from which YES is more expensive:

```python
if poly_yes > kalshi_yes:  poly_side, kalshi_side = "NO", "YES"
else:                      poly_side, kalshi_side = "YES", "NO"
```

You always sell/avoid the expensive YES and buy the cheap one on the other venue.

---

## 3. The "two-sided quote" assumption (important)

A single recorded trade only happened on **one** side. For example, the poly trade
`…;473` at `1753244920` was a **YES** trade of 319 shares at 0.715. But the arb
above needs to buy poly **NO** at 0.285.

Her method treats every trade as a **two-sided price quote**: seeing a YES trade
at 0.715 means "the market was at 0.715 / 0.285, so I assume I could have taken
*either* side at that price." The matcher therefore checks **only the price gap**,
never the trade's actual outcome, when deciding whether trades can arb.

Consequences:
- A trade can be matched on the leg **opposite** to how it actually traded.
- In `matched_trades.csv` the `side` column reports the trade's **real outcome**
  (YES for `…;473`), even though the arb leg was NO. The two-sided assumption is
  what links them.

> **What this buys and costs.** It roughly doubles the matchable liquidity (both
> sides of every quote are "available"), which is realistic for a liquid,
> two-sided book — but it's an *inference*: a YES print is not proof that NO depth
> existed at that instant. This is the single biggest optimism in the model.

---

## 4. Data preparation

Before matching, each venue's trades are cleaned and sorted (`arbitrage.py`
lines 26–201):

1. **Floor Polymarket sizes to whole contracts.** `poly_size = floor(shares)`.
   Fractional-share trades below 1.0 floor to 0 and are dropped entirely (no
   liquidity, no row). Kalshi trades are already integer counts.

2. **Normalize prices to a YES basis.** For a poly trade, if its outcome is YES
   the price *is* the YES price; if NO, `yes_price = 1 − price`. `poly_yes` is then
   rounded to 3 decimals (this rounded value is what the arb test compares).

3. **Sort each venue for deterministic consumption order:**
   - Kalshi: `(timestamp, trade_id string)`
   - Poly: `(timestamp, tx string, logIndex int)`

   This tie-break matters: when many trades share a timestamp, it fixes *which*
   one is consumed first, so the output is reproducible.

4. **A merged, sorted file** (`kalshi_poly_merged_sorted_for_arb.csv`) interleaves
   both venues by `(timestamp, id)` — this is the canonical input the matcher
   walks.

---

## 5. The matching algorithm

The matcher is a **synchronized two-pointer sweep** (`arbitrage.py` lines
234–409): pointer `i` over the sorted Kalshi list, pointer `j` over the sorted
Poly list. Four properties define it:

### (a) The *earlier* trade drives
Whichever of `K[i]` / `P[j]` has the **earlier timestamp** becomes the **active**
trade and consumes liquidity from the other venue. Poly can drive Kalshi just as
Kalshi drives Poly — it is **not** a one-sided "Kalshi hunts Poly" loop.

```python
if K[i]["kalshi_time"] <= P[j]["poly_time"]:   # active = Kalshi
    ...
else:                                          # active = Poly
    ...
```

### (b) Forward-only window
The active trade at time `t` matches partners with timestamp in **`[t, t + 300s]`**
— i.e. the partner must arrive **within 5 minutes after** the active trade. Since
the active trade is always the earlier one, this is equivalent to "the two trades
are within 5 minutes of each other," anchored to the earlier one.

### (c) Both sides deplete
Every match subtracts `arb_size` from **both** the active and partner remaining
size. A trade with `size ≤ 0` is skipped forever. This is a **consumption** model,
not a reusable-quote model: once liquidity is spent, later trades can't reuse it.

### (d) First-partner, greedy, repeated
For the active trade, scan forward for the **first** opposite-venue trade with
`size > 0` and a price gap; match `arb_size = min(active_size, partner_size)`;
repeat with the next partner until the active trade is exhausted **or** no partner
remains in its window (in which case its remainder is discarded).

```python
arb_size = min(active_size, partner_size)
active_size  -= arb_size
partner_size -= arb_size
```

---

## 6. Worked example (real data)

The very first arbitrage in the file. From the merged input at
`t = 1753244920` and `t = 1753245085`:

```
1753244920  poly    …;473   YES  319   yes 0.715 / no 0.285
1753244920  poly    …;476   NO    11   yes 0.715 / no 0.285
1753245085  kalshi  7fc4…    no    27   yes 0.710 / no 0.290
```

**Step by step:**

1. Poly `…;473` (t = …920) is earlier than the Kalshi trade (t = …085), so **poly
   is active**. Its window is `[…920, …920 + 300] = […920, …1220]`.
2. Scan forward for a Kalshi partner in that window → the `7fc4…` trade at
   `…085`. Price gap: `|0.715 − 0.710| = 0.005 ≥ EPS` ✓.
3. `arb_size = min(poly 319, kalshi 27) = 27`. Consume both:
   poly `…;473` → 292 left, kalshi `7fc4…` → 0 (exhausted).
4. Sides: `poly_yes 0.715 > kalshi_yes 0.710` → **poly NO, kalshi YES**. The
   locked trade is *buy poly NO @ 0.285 + buy kalshi YES @ 0.710 = 0.995* →
   **0.5¢ × 27 = $0.135 profit**.
5. Poly `…;473` still has 292 left, but no other Kalshi trade falls inside its
   window, so its remainder is dropped. Pointer advances to poly `…;476`.
6. Poly `…;476` becomes active, but the only in-window Kalshi trade (`7fc4…`) is
   now depleted → **no partner → `…;476` is discarded** (never appears in output).

**This is exactly what the reference files show:**

- `arbitrage_final.csv`: `poly_time=…920, kalshi_time=…085, arb_size=27,
  poly_side=NO, kalshi_side=YES`
- `matched_trades.csv`: emits poly `…;473` (YES, 319) and kalshi `7fc4…` (no, 27)
  — **and not** `…;476`.

Note the two outputs describe the same event differently: `arb_size = 27` (the
matched quantity) vs. the per-trade log's **319 and 27** (the *full original
sizes* of the trades that participated).

---

## 7. Why depletion (not reuse) — and how it shows up

Because both sides deplete, **Kalshi is the scarce, binding side**: total Kalshi
volume is far smaller than Polymarket's, so most Kalshi contracts get consumed
while most Poly trades never do. Two visible consequences:

- **Some Kalshi trades go unmatched** — by the time they're processed, the Poly
  liquidity in their window has already been spent by earlier Kalshi trades.
- **A large Poly trade is usually only partially consumed**, so at a busy price
  level only the first trade or two are recorded (the rest are never needed).

> *Contrast with a "reuse" model*, where one standing Poly quote could satisfy
> many Kalshi trades. That would inflate matched volume ~2× and record far fewer
> distinct Poly trades. Depletion is what makes the counts land where they do
> (≈80.4k poly / 42.2k kalshi matched trades).

---

## 8. The two output files

| File | Grain | One row = | Key columns |
|---|---|---|---|
| `arbitrage_final.csv` | one **match** | a poly-leg ⇄ kalshi-leg pairing | `poly_time, kalshi_time, poly_yes, poly_no, kalshi_yes, kalshi_no, arb_size, poly_side, kalshi_side` |
| `matched_trades.csv` | one **trade** | a distinct source trade that was consumed ≥ once | `timestamp, market, trade_id, side, size, yes_price, no_price` |

In `matched_trades.csv`:
- `market` ∈ {`poly`, `kalshi`}; poly `trade_id` = `<tx_hash>;<logIndex>`, kalshi
  = the UUID.
- `side` = the trade's **raw outcome** (poly upper-case, kalshi lower-case) — the
  side it actually traded, *not* the arb leg.
- `size` = the trade's **full, floored original** quantity (not `arb_size`).
- Rows are sorted by `(timestamp, market, original-input-index)`.

---

## 9. Assumptions & limitations (the honest list)

1. **Two-sided quoting** (§3): assumes both sides of every quote were executable.
   Optimistic; a print evidences only one side.
2. **Hypothetical fills**: "the discrepancy existed" ≠ "you could have filled the
   full size." Even a real HFT bot is limited by resting depth.
3. **±5-minute window**: a fixed tolerance for "simultaneous." Trades further
   apart never match, even if the mispricing persisted.
4. **Floor to whole contracts**: discards sub-1-share poly liquidity; a small
   downward bias on volume.
5. **`EPS = 1e-4` on 3-dp-rounded poly prices**: gaps below a tenth of a cent are
   ignored, and rounding can nudge borderline pairs.

None of these are bugs — they're the deliberate simplifications that turn a messy
order book into a clean "was there capturable mispricing?" signal.

---

## 10. Reproducing it

`src/build_arbitrage_matched_trades.py` ports the two-pointer matcher and runs it
on `kalshi_poly_merged_sorted_for_arb.csv`. It reproduces `matched_trades.csv`
with **0 missing / 0 extra** across all 122,635 rows (content-identical; only the
within-second row *ordering* can differ, because her output order uses a
`tx-maker-taker` index not present in the merged file). Sort both files by
`(timestamp, market, trade_id)` and they are identical.
