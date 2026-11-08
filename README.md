# arb — cross-venue prediction-market data & arbitrage

Trade-level data collection for the same real-world event listed on **Kalshi** and
**Polymarket**, and a reproduction of the two-pointer arbitrage matcher used in
the original study.

Everything is **event-driven**: one folder per market pair under `events/<slug>/`,
one `event.json` that every stage reads. The same code runs on any event.

---

## Events

| slug | Kalshi | Polymarket | Net (arb) | Poly source |
|---|---|---|---|---|
| `presidential-2024` | `PRES-2024-DJT` | Trump wins the presidency | **$3,049,168** | subgraph |
| `presidential-2024-v2` | `PRES-2024-DJT` | *(same market, re-collected)* | **$3,049,253** | data-api v2 |
| `popvote-2024` | `POPVOTE-24-R` | Trump wins the popular vote | **$490,526** | subgraph |
| `mamdani-nyc-mayor` | `KXMAYORNYCPARTY-25-D` | Mamdani wins NYC mayor | $390,993 | data-api v2 |
| `mamdani-dem-nomination` | `KXMAYORNYCNOMD-25-ZM` | Mamdani wins the primary | $26,186 | data-api v2 |
| `popvote-2024-biden` | *(none in window)* | Biden wins the popular vote | — | subgraph |
| `popvote-2024-kamala` | *(none in window)* | Harris wins the popular vote | — | subgraph |

`presidential-2024-v2` is the **same market as `presidential-2024`, collected
again from a completely independent source** after the subgraph was retired.
Both events are kept, because the pair is the strongest validation in this
repo:

| | subgraph (maker view) | data-api v2 (taker view) |
|---|---|---|
| Polymarket rows | 2,749,706 | 2,359,913 |
| Polymarket shares | 1,568,716,366.53 | 1,568,716,366.52 |
| Net | $3,049,168 | $3,049,253 |

Row counts differ by 14% (different grouping), while **share volume agrees to
0.01 of a share in 1.57 billion and the Net to 0.003%**. The two tapes also
agree transaction-by-transaction: identical 2,359,913 `tx_hash` values, none
unique to either side, all agreeing on size. See
`notes/polymarket_v2_migration.md`.

The two Mamdani events were re-collected from the Data API v2 after the
Goldsky subgraph was retired; their Nets moved **-0.41%** and **+0.97%** from
the subgraph-sourced figures ($392,592 and $25,934). That gap is the taker-order
vs maker-fill row granularity, not a change to the matcher — see
`notes/polymarket_v2_migration.md`. The two 2024 events still carry their
original subgraph tapes and numbers.

> **Both presidential tapes sit 2.43% above Gamma's `volumeNum`, and that is
> GAMMA's error, not theirs.** This was an open question until the v2
> re-collection settled it: two fully independent sources produced the same
> 1,568,716,366.5 shares, agreeing transaction-by-transaction, while Gamma
> reports 1,531,479,285. Two independent pipelines do not converge to ten
> significant figures on the same wrong number.
>
> `validate_event.py` therefore **warns** rather than fails when a tape exceeds
> Gamma, and still **fails** when it falls short — only a short tape can
> indicate the truncation that check exists to catch. Both presidential events
> pass validation. See `notes/polymarket_v2_migration.md`.

The two `popvote-2024-{biden,kamala}` events are **deliberate time slices**
(2024-06-01 → 2024-09-01), collected only to timestamp the Biden→Harris switch.
They are not complete market histories — see their `event.json`.

## Quick start — one command, any market pair

Give it the two markets; it returns the whole dataset and the charts.

```bash
python src/run_event.py \
  --kalshi KXMAYORNYCPARTY-25-D \
  --poly   https://polymarket.com/event/nyc-mayor/will-zohran-mamdani-win-the-2025-nyc-mayoral-election
```

`--poly` takes a Polymarket URL, a market slug, or a `0x…` condition id. The
Kalshi ticker and the Polymarket market are looked up in the venues' metadata
APIs, `events/<slug>/event.json` is written for you, and every stage runs in
order. Re-run a known event with `python src/run_event.py <slug>`, or sweep all
of them with `--all`.

A stage whose output exists is skipped, so a re-run is cheap and an interrupted
run resumes. `--force` re-runs the derived stages but never re-downloads or
deletes raw data.

### Long runs and resume

The big markets take a while — `presidential-2024` is 287k Kalshi trades and
over a million Polymarket fills — so **both fetchers checkpoint after every
page**. If a run is killed (dropped connection, closed terminal, restarted
machine), just run the same command again: each fetcher picks up from its own
checkpoint rather than re-paging from the start, and the duplicate guard makes
the resumed tape identical to an uninterrupted one.

A leftover `.checkpoint.json` next to a raw tape means **that tape is partial**.
The fetchers delete the checkpoint on clean completion, and `run_event.py`
treats its presence as "not done" — so a half-finished tape is never silently
handed to the cleaner. Don't delete a checkpoint by hand unless you also delete
its tape; that combination is the one way to turn a partial tape into a
permanently truncated one.

> **If you touch the resume code, keep the tail re-read.** A checkpoint is
> written *after* a page, so a process killed **mid-page** leaves rows on disk
> that no checkpoint knows about (the write buffer flushes every ~16 rows). The
> resumed run replays that page from the stored cursor, and without a dedupe
> set covering those orphan rows it writes every one of them a second time.
> Both fetchers therefore prime their dedupe set from the last `2 * limit` rows
> of the existing tape on resume (`eventlib.tail_json_lines`). Verified: a
> mid-page kill that stranded 500 unrecorded rows resumed to a tape identical
> to an uninterrupted fetch, with zero duplicates. Testing only a kill at a
> page *boundary* will not catch a regression here.

### Deliverables

All under `events/<slug>/results/`:

| File | What it is |
|---|---|
| `kalshi_sorted_for_arb.csv` | Kalshi's tape, pre-arb |
| `poly_sorted_for_arb.csv` | Polymarket's tape, pre-arb |
| `kalshi_poly_merged_sorted_for_arb.csv` | both venues interleaved (matcher input) |
| `merged_sorted_for_arb.csv` | same content, short name |
| `matched_trades.csv` | every trade the matcher consumed |
| `arbitrage_final.csv` | one row per matched arb pair |
| `results.txt` | per-venue P&L and the Net |
| `charts.html`, `prices.png`, `positions.png`, `locked_in.png` | the visualizations |

A machine-readable index of the last run lands at
`events/last_run_manifest.json`.

## Pipeline

```
resolve_market.py             market ids -> events/<slug>/event.json
fetch_kalshi_trades.py        Kalshi historical trades    -> raw/*.jsonl
fetch_polymarket_dataapi.py   Polymarket trades (v2 API)  -> raw/*.jsonl[.gz]
clean_kalshi_trades.py        raw -> clean/*.csv   (shared cross-venue schema)
clean_polymarket_dataapi.py   raw -> clean/*.csv   (same schema)
validate_event.py             reconcile both tapes against independent sources
merge_for_arb.py              clean tapes -> the 4 sorted-for-arb CSVs
run_arbitrage.py              -> results/arbitrage_final.csv + matched_trades.csv
results_summary.py            -> results/results.txt
make_charts.py                -> results/charts.html + 3 PNGs
analyze_sync.py               cross-venue lead-lag / sync timing (diagnostic)
compare_runs.py               two events, same market pair, different sources
run_event.py                  runs all of the above in order
```

`compare_runs.py` exists for the re-collections forced by the V2 migration:

```bash
python src/compare_runs.py presidential-2024 presidential-2024-v2
```

It checks the two events' configs match first (a differing `start_ts` alone can
move the Net by double digits), then compares share volume — the invariant that
survives a source change — and only then the Net. **Row counts are expected to
differ ~2x and mean nothing on their own**; a share-volume gap above 1% means a
tape is incomplete rather than merely coarser. Exits non-zero on disagreement,
so it can gate a re-collection.

Stage by stage, if you want to drive it manually:

```bash
python src/resolve_market.py --kalshi <TICKER> --poly <url|slug|0x…>
python src/fetch_kalshi_trades.py       <slug>
python src/clean_kalshi_trades.py       <slug>
python src/fetch_polymarket_dataapi.py  <slug>
python src/clean_polymarket_dataapi.py  <slug>
python src/validate_event.py            <slug>
python src/merge_for_arb.py             <slug>
python src/run_arbitrage.py             <slug>
python src/results_summary.py           <slug>
python src/make_charts.py               <slug>
```

### Deprecated Polymarket paths

`fetch_polymarket_subgraph.py`, `combine_subgraph_parts.py` and
`clean_polymarket_subgraph.py` read the **Goldsky orderbook subgraph, which
Polymarket's V2 migration retired (2026-11-04)**. The endpoint now returns
`ENDPOINT_DEPRECATED` and warns that any data it does serve is stale. The Data
API v1 is likewise HTTP 410. Those scripts are kept only so pre-migration tapes
still clean and reproduce; **do not use them to collect new data.**

**Kalshi did not change** — its trades endpoint and schema are as before.

Read **`notes/polymarket_v2_migration.md`** before touching the Polymarket
side. It documents the replacement endpoint, four traps that silently return
the wrong data (most importantly: *the v2 cursor does not carry the market
filter*, so a paged fetch that forgets to re-send it truncates to one page and
still reports success), and the measured differences between the old and new
tapes.

## Layout

```
events/<slug>/
  event.json    market ids, exchange, window, resolution, notes   [versioned]
  raw/          verbatim API/chain records, never modified         [gitignored]
  clean/        trade-level CSVs, shared schema                    [gitignored]
  results/      arb pairs, matched trades, results.txt, charts     [versioned]
  reference/    outputs to compare against (e.g. the RPC-derived run)
src/            the pipeline (see above)
notes/          data dictionary, validation, sync timing
docs/           arbitrage matching methodology
```

`raw/` and `clean/` are gitignored (too large) but **fully regenerable** from
`event.json` via `src/fetch_*` + `src/clean_*`. `results/merged_sorted_for_arb.csv`
is likewise regenerable and not versioned.

Large Polymarket tapes are stored **gzipped** (`"poly_raw_gzip": true` in
`event.json`). The records are byte-identical — one verbatim JSON object per line —
only the container is compressed. presidential-2024: 2.8 GB → 424 MB.

## Read these before trusting a number

- **`docs/arbitrage_matching_methodology.md`** — how the matcher works, and its
  assumptions. The big one: every trade is treated as a **two-sided quote**, i.e.
  a YES print at 0.715 is assumed to mean NO was takeable at 0.285. That is an
  inference, not evidence, and it is the single largest optimism in the model.
  **The Net figures above are upper bounds, not realised P&L.**
- **`notes/sync_timing.md`** — dislocations between the venues decay with a
  half-life of **~97s** when the market is active, but `window_seconds` is **300**.
  Inside 300s a single leg can move 3–6¢ while the whole edge is ~3¢. Run a
  sensitivity sweep over the window before quoting a profit.
- **`notes/data_dictionary_polymarket.md`** — the shared clean schema, plus three
  decode traps that were live bugs in this repo:
  - Polymarket `size` is **USDC, not shares** → `quantity = (size/1e6)/price`.
  - The subgraph's `side` on a maker-fill row is the **maker's**; flip it to get
    the taker (aggressor) convention.
  - Kalshi has **no buy/sell field** at all; `side` is always `BUY` and `outcome`
    carries the direction.

## Data provenance

Polymarket is collected from the **Data API v2**
(`data-api.polymarket.com/v2/trades`, no API key), one row per taker order.
Each tape is reconciled two ways, both against counters computed independently
of the trades we paged: our share total vs Gamma's `volumeNum` (tolerance 1%;
observed 1.0000 and 0.9990 on the two Mamdani markets), and our newest trade vs
the API's newest trade. The second check is not ceremony — the v2 cursor does
not carry the market filter, so a fetch that drops it truncates to a single page
while still reporting `has_more: false`, i.e. success.

Earlier tapes came from the Goldsky orderbook subgraph, reconciled against that
indexer's `orderbook.tradesQuantity`. That endpoint was retired in Polymarket's
V2 migration and now warns its own data is stale.

On `presidential-2024` the subgraph-derived arbitrage reproduces the earlier
RPC-derived run to **0.024%** on Net, so the missing on-chain `logIndex` — which
only affects within-second consumption order — is immaterial.

An Alchemy RPC path (`fetch_polymarket_rpc.py`) also exists and is the only source
of a true `logIndex`; it needs `ALCHEMY_POLYGON_URL` in `.env`.
