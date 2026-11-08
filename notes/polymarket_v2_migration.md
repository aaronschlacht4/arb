# Polymarket V2 migration — what broke, what replaced it

**Date discovered: 2026-11-04.** Both Polymarket collection paths this repo used
are dead. Kalshi is unaffected. Everything below was verified against the live
APIs, not read from docs.

## Which venue changed: Polymarket, not Kalshi

**Kalshi is unchanged.** `GET /trade-api/v2/historical/trades?ticker=<T>` still
returns the same schema the cleaner already expects — `count_fp`,
`yes_price_dollars`, `no_price_dollars`, `taker_outcome_side` — and the stored
raw tapes from the earlier runs are byte-comparable to a fresh pull. No change
was needed on the Kalshi side of the pipeline.

**Polymarket broke in two places at once:**

| Dead endpoint | Response |
|---|---|
| Goldsky subgraph `polymarket-orderbook-resync` | HTTP 429 + `ENDPOINT_DEPRECATED`: *"This Polymarket subgraph endpoint is paused and deprecated following Polymarket's migration to V2 — the data is stale and incorrect. Stop using it."* |
| Data API **v1** `data-api.polymarket.com/trades` | HTTP 410 `{"error": "Data API v1 has been retired"}` |

Note the subgraph does not merely 404 — where it answers at all, the data is
stale. A pipeline that silently kept reading it would produce plausible,
*wrong* numbers. That is the main reason the completeness checks below exist.

## The replacement: Data API v2

```
GET https://data-api.polymarket.com/v2/trades
    ?condition_id=<0x…>&limit=1000&taker_only=true[&cursor=<opaque>]
```

No auth, no API key, free.

**Throughput varies by about 10x and you cannot plan around the fast case.**
Measured on the same endpoint, same page size, hours apart:

| | per 1000-row page | effective |
|---|---|---|
| fast (2026-11-04) | ~0.6 s | ~1,600 rows/s |
| slow (2026-11-05) | ~7 s | ~140–220 rows/s |

The slow case still returns HTTP 200 with full 1000-row pages — it is
server-side throttling or load, **not** an error, so there is nothing to retry
and nothing in the response that flags it. A 1M-row market is therefore
somewhere between 10 minutes and 2 hours. Budget for the slow case, and lean on
the checkpoint rather than trying to finish in one sitting. Running a second
fetch (or even a latency probe) against the API concurrently competes for the
same budget and slows the real one down.

Response: `{"data": [...], "pagination": {limit, offset, has_more, next_cursor}}`.
Trades come newest → oldest, timestamps monotonically non-increasing.

Row fields: `transaction_hash`, `timestamp`, `price`, `size`, `side`,
`token_id`, `condition_id`, `outcome`, `outcome_index`, `proxy_wallet`,
`title`, `slug`, `event_slug`.

### Four traps, each verified

1. **The cursor does not carry the market filter.** This is the dangerous one.
   The cursor decodes to a page size and a `(block_timestamp, sequence_id)`
   anchor plus a signature — no filter. A request with the cursor **alone**
   returns 0 rows and `has_more: false`, which is *indistinguishable from
   end-of-data*. A naive cursor-follow therefore collects exactly one page and
   then reports success with a 1000-trade tape.
   → `arbpipe.fetch.polymarket_dataapi` re-sends `condition_id` **and**
   `taker_only` on every single page, and `arbpipe.verify.validate` checks the tape's
   head against the API's head to catch a regression.

2. **`asset_id` is silently ignored.** Filtering by token id returns HTTP 200
   and a page of a *completely unrelated market* — we asked for the Mamdani
   token and got a Champions League over/under. Exactly the same shape as
   Kalshi's old `market_ticker` trap. `market=` is at least rejected with a
   helpful 400. → we re-verify `condition_id` on every row and abort on
   contamination.

3. **Time filters are accepted and ignored.** `start_ts`, `end_ts`, `from`,
   `to`, `min_timestamp` all return 200 and change nothing; a request bounded
   to before the market opened still returns the newest trades. There is
   therefore **no way to shard this source by time**, unlike the subgraph
   fetcher. Not a problem in practice — the whole tape is minutes.

4. **Three-year window.** `start`/`end` are honored only on the `user` query
   shape. Filtering by condition serves a fixed ~3-year window, so a market
   that stopped trading before roughly Nov 2023 returns **0 rows**. A 2021
   market was verified empty. Every event in this repo is inside the window.

## `taker_only` — which view we take, and why

- `taker_only=true` (the default, and what we pin explicitly): **one row per
  economic trade**, from the taker's side.
- `taker_only=false`: additionally emits the **counterparty row** for each
  trade — same tx, same size, same price, opposite side, the maker's wallet.
  It is an exact doubling, not extra information. Summing it double-counts
  every dollar of volume.

We pin `taker_only=true` rather than relying on the default, so that a change
to the API's default cannot silently double our tape.

## Why the new cleaner is simpler

`arbpipe.legacy.clean_polymarket_subgraph` undoes three artifacts of the old orderbook feed.
**None applies to v2, and applying any of them would corrupt the tape:**

| Subgraph correction | v2 reality |
|---|---|
| Drop aggregate rows where `taker == Exchange` | No aggregate rows exist. `polymarket_exchange` is not consulted at all. |
| `size` is 6-decimal USDC → `quantity = (size/1e6)/price` | `size` is **shares**, already decimal (e.g. `78.54`). `quantity = size`, `notional = size*price`. |
| `side` is the *maker's* → flip it | `side` is already the **taker's**. Flipping would invert every trade. |

## Known differences from the old tape (not bugs)

The v2 taker view is **coarser** than the subgraph's maker-fill view: one row
per taker order instead of one row per matched maker order. Measured on
`mamdani-dem-nomination`, same market and same window (the old 2025-01-02
start; the event has since moved to Wendy's 2025-05-27 start, see README):

| | old (subgraph) | new (data-api v2) |
|---|---|---|
| Polymarket rows | 40,823 | 21,429 |
| shares (floored) | 17,832,146 | 17,843,423 |
| timestamp range | 1735575578..1751086220 | identical |

**Total share volume agrees to 0.06%** — the economic content is the same — but
the row count roughly halves. Because the matcher consumes chunkier Polymarket
rows, the matched set differs and so does the headline number:

| | old | new |
|---|---|---|
| Net | $25,934 | $26,186 (+1.0%) |
| matched_trades rows | 31,263 | 14,146 |

Across all four re-run events the Net lands within ~1% of the pre-migration
figure, and the two largest markets land within 0.1%:

| event | old Net (subgraph) | new Net (v2) | delta |
|---|---|---|---|
| `mamdani-nyc-mayor` | $392,592 | $390,993 | **-0.41%** |
| `mamdani-dem-nomination` | $25,934 | $26,186 | **+0.97%** |
| `presidential-2024` | $3,049,168 | $3,049,253 | **+0.003%** |
| `popvote-2024` | $490,526 | $490,150 | **-0.08%** |

The delta shrinks as the market gets bigger, which is what the granularity
explanation predicts: on a market with millions of trades the chunkier rows
average out, while on a thin one a single re-grouped row can move the match.
Do not read the two small deltas as "worse data" — the **share volume is the
invariant**, and it agrees on every one of the four:

| event | poly rows old -> new | shares old -> new | share delta |
|---|---|---|---|
| `mamdani-dem-nomination` | 40,823 -> 21,429 | 17,832,146 -> 17,843,423 | 0.06% |
| `presidential-2024` | 2,749,706 -> 2,359,913 | 1,568,716,366.53 -> 1,568,716,366.52 | 0.0000% |
| `popvote-2024` | 197,428 -> 168,432 | 119,932,841 -> 119,933,283 | 0.0004% |

Kalshi is untouched by the migration and re-fetches byte-for-byte identically
(`popvote-2024`: 37,480 rows and 36,740,947 contracts both times, 0.000%).

This is a **real methodological difference**, not drift: fewer, larger
Polymarket rows means less granular depletion in the two-pointer match. The
Net figures across the two sources are therefore comparable but not identical,
and a number sourced from v2 should not be diffed against a pre-migration one
as if it were a regression.

### A trap this exposed: `start_ts` is not optional

Re-running `mamdani-nyc-mayor` **without** its `start_ts` cutoff gives
$401,645 instead of $390,993. The Kalshi contract there is *"a Democrat wins
the NYC mayor race"*, which before the June 2025 primary is **not the same
question** as *"Mamdani wins"* — Kalshi sat near 0.90 while Polymarket sat near
0.20. The matcher reads that 70-point basis as enormous free money.
`start_ts: 1750824000` exists to cut that period off.

The tell is in `arbpipe.verify.validate`'s overlap report: a max daily gap of **+0.86**
rather than a few cents. A large one-sided gap means the two contracts are not
the same question, not that you found an arbitrage.

## Independent completeness check (replaces `tradesQuantity`)

The subgraph's `orderbook.tradesQuantity` counter is gone with the endpoint. The
replacement ground truth is Gamma's own rollup:

```
GET https://gamma-api.polymarket.com/markets?condition_ids=<0x…>&closed=true
```

`volumeNum` is in **shares**, computed by Gamma's indexer independently of the
trades we paged. On `mamdani-dem-nomination`: our tape 17,848,934 shares vs
Gamma 17,848,349 — ratio 1.0000. `arbpipe.verify.validate` fails above 1% drift.

Gamma agrees closely on three of the four markets, which is what makes the one
disagreement interpretable rather than just noise:

| event | our shares | Gamma `volumeNum` | gap |
|---|---|---|---|
| `mamdani-dem-nomination` | 17,848,934 | 17,848,349 | +0.003% |
| `popvote-2024-v2` | 119,933,283 | 119,932,621 | +0.0006% |
| `presidential-2024-v2` | 1,568,716,367 | see below | **+2.43%** |

So Gamma's rollup is not broken in general — on `popvote-2024-v2` it is within
662 shares of 120 million. The presidential gap is specific to that market,
which is why the resolution below pins it on Gamma's counter for that one
market rather than on our collection method.

> **`volumeNum` is SHARES, not dollars — and that has already caused one error
> in this repo.** `presidential-2024`'s `event.json` described the market as
> "Polymarket volume ~$1.53B", which is Gamma's *share* count read as dollars.
> The market's actual dollar notional is **$818M**: 1.53B shares at an average
> price of 0.52. Both numbers are now in that `event.json`, labelled. Whenever
> you quote a Polymarket "volume", say which unit you mean — for a market that
> traded near 50c the two differ by about 2x.

Two Gamma traps: the **singular** `condition_id` is silently ignored and returns
unrelated markets (so read `conditionId` back off every row), and both `slug`
and `condition_ids` search only *open* markets unless `closed=true` is passed —
a settled market otherwise looks nonexistent.

### The direction of the gap is the diagnosis

A truncated fetch can only **lose** volume; it can never invent any. So the two
directions mean different things, and `arbpipe.verify.validate` treats them
differently:

- **shares < Gamma** — trades are missing. **FAIL.** This is the truncation
  case the check exists for.
- **shares > Gamma** — Gamma's counter is the low one. **WARN only.** See the
  resolved case below for why this is not a failure.

### RESOLVED: the presidential-2024 "2.43% excess" is Gamma's, not ours

**Status: settled 2026-11-05. Our tapes were correct; Gamma under-reports this
market.**

The symptom: `presidential-2024`'s Polymarket tape carried 2.43% more share
volume than Gamma's `volumeNum`, where every other event agreed to within 0.1%.
That looked like a defect in the tape, and three hypotheses were chased and
ruled out (shard-boundary duplication, undropped aggregate rows, neg-risk
accounting — all disproven; see the history below).

**The decisive test was collecting the same market twice, from two fully
independent sources**, which is possible only here and only once, because the
subgraph is now dead:

| | subgraph (maker-fill view) | data-api v2 (taker view) |
|---|---|---|
| rows | 2,749,706 | 2,359,913 |
| **shares** | **1,568,716,366.53** | **1,568,716,366.52** |
| USDC notional | 818,020,164.50 | 818,168,054.89 |

Different endpoints, different row granularity, different decode paths, written
16 months apart — and they agree on **1.57 billion shares to one hundredth of a
share** (0.000000%). Both sit 2.43% above Gamma's 1,531,479,285.

Two independent pipelines do not converge to ten significant figures on the
same wrong number. **Gamma's rollup under-counts this market.** The tapes are
right.

And the agreement is not merely aggregate — it holds **transaction by
transaction**. Summing shares per `tx_hash` in both tapes:

| | |
|---|---|
| transactions in each tape | 2,359,913 |
| present in only one tape | **0** |
| agreeing on shares to within 0.1% | **2,359,913 (all of them)** |
| total excess | **0 shares** |

So the maker-fill view and the taker view describe the same trades, with the
same sizes, in the same transactions — they differ only in how those fills are
grouped into rows (2,749,706 vs 2,359,913). There is no subset of trades on
which the two sources disagree at all.

Two corollaries worth keeping:

- `presidential-2024`'s **$3,049,168 no longer carries an unexplained volume
  excess** — it is corroborated by a second, independent collection.
- The v2 row count, **2,359,913, exactly equals the number of distinct
  `tx_hash` values in the subgraph tape** (predicted from the baseline hours
  before the fetch finished). The v2 feed emits precisely one row per
  transaction; the maker view emits 1.165.

**So `arbpipe.verify.validate` only FAILS on `shares < Gamma`.** Failing on an excess
would mean rejecting correct data on a worse oracle's word. If you hit an
excess on some other market, do what was done here: re-collect from a second
source and trust agreement between two over either one against Gamma.

#### How the three wrong hypotheses were excluded

Kept because each took real work, and because a future excess should not
re-tread them:

- *Shard-boundary double-counting.* `presidential-2024` is the only tape
  fetched in parallel time shards, but `arbpipe.legacy.combine_subgraph_parts` dedupes by
  record `id` and the clean tape has **zero duplicate trade_ids**.
- *neg-risk accounting.* `popvote-2024` clears through the same neg-risk
  exchange and matches Gamma exactly.
- *Undropped aggregate rows.* Excluded twice over. **By address:** neither
  exchange contract appears as a `taker` anywhere in the 2.75M-row tape. **By
  shape:** a surviving aggregate leaves one row equal to the sum of the others
  in its transaction; across 176,120 multi-row transactions only 477 match that
  shape, worth 1.3M shares (0.08%) — and an even two-maker split matches it
  too, so that is an upper bound. The gap to explain was 37.2M shares (2.37%),
  about 30x larger.
- *One dominant actor.* The largest single taker holds 35.5M shares, close to
  the 37.2M excess — but it is an ordinary trader across 5,989 rows, and the
  top 8 takers together are under 11% of volume.

## Metadata resolution (for `arbpipe.resolve`)

| Need | Endpoint | Trap |
|---|---|---|
| Kalshi market | `/trade-api/v2/historical/markets?tickers=<T>` | Filter is **`tickers`, plural**. The singular `ticker` is ignored and returns unrelated markets — the *opposite* of `/historical/trades`, where the filter *is* singular `ticker`. Settled markets are absent from the live `/markets`. |
| Polymarket market | `https://clob.polymarket.com/markets/<conditionId>` | Authoritative, works for settled markets. `tokens` carries each outcome's token id plus a `winner` flag → `resolution`. |
| slug → condition id | `gamma-api…/markets?slug=<s>&closed=true` | Needs `closed=true` for settled markets. |
| condition → token pair | `data-api…/v2/tokens?condition=<0x…>` | Also gives `neg_risk`, `final_price`, `resolved`. |

`arbpipe.resolve` orders token ids by each token's own `outcome` label, never
by list position: a market that listed No first would otherwise invert the whole
tape, turning every price `p` into `1-p`.

## If v2 also goes away

Goldsky's paid **Feeds / Edge Data API** is the documented successor for
activity data: `https://edge.goldsky.com/data/polymarket/activity`, keyed by
`token_id` (no condition filter), needs an `x-api-key`, billed 1 CU per row at
$10/million CU (~$6 per 600k fills). Its `ActivityItem` is the closest match to
the old subgraph schema — it is the only candidate with a true fill `id`, an
explicit `order_type` of maker/taker, and `counterparty_address`, and the only
one reaching pre-2023 history. Its spec flags a known data-quality issue on
`fee`. Not used here because v2 is free, unauthenticated, and sufficient.
