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
| `mamdani-dem-nomination` | `KXMAYORNYCNOMD-25-ZM` | Mamdani wins the primary | $25,508 | data-api v2 |
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

`mamdani-dem-nomination` now starts at **2025-05-27 00:00 ET**
(`start_ts` 1748318400), matching Wendy's run; it previously started
2025-01-02 and netted $26,186.

**Our scripts reproduce Wendy's byte for byte.** Fed her raw Kalshi JSON and
on-chain OrderFilled CSV, our clean, merge and match stages write all five of
her per-window files (merged, both sorted tapes, `arbitrage_final_*`,
`matched_trades_*`) byte-identical to hers at 1, 5 and 10 minutes. That is a
test (`tests/test_wendy_parity.py`) and a stage:

```bash
python arb.py parity mamdani-dem-nomination --her wendysResults/mamdani-dem-nomination --her-raw
python arb.py parity mamdani-dem-nomination --her wendysResults/mamdani-dem-nomination   # where OUR data differs
```

So any difference in results is the data. Her Kalshi trades are identical to
ours. The remaining gap is Polymarket row granularity: she uses on-chain maker
fills, we use Data API taker orders.

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
python arb.py run \
  --kalshi KXMAYORNYCPARTY-25-D \
  --poly   https://polymarket.com/event/nyc-mayor/will-zohran-mamdani-win-the-2025-nyc-mayoral-election
```

`arb.py` is the only entry point you need; it puts `src/` on `sys.path`, so
**nothing has to be installed first**. `python arb.py` on its own lists
every stage.

`--poly` takes a Polymarket URL, a market slug, or a `0x…` condition id. The
Kalshi ticker and the Polymarket market are looked up in the venues' metadata
APIs, `events/<slug>/event.json` is written for you, and every stage runs in
order. Re-run a known event with `python arb.py run <slug>` (or just
`python arb.py <slug>`), or sweep all of them with `--all`.

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

Every event folder has the same layout, numbered in the order the data flows
(see `events/README.md`):

| Folder | File | What it is |
|---|---|---|
| `1_raw/kalshi/`, `1_raw/polymarket/` | `*.jsonl` | as fetched, never modified (gitignored) |
| `2_clean/kalshi/`, `2_clean/polymarket/` | `*.csv` | one trade per row, shared schema (gitignored) |
| `3_sorted/` | `kalshi_sorted_for_arb.csv` | Kalshi's tape, pre-arb |
| | `poly_sorted_for_arb.csv` | Polymarket's tape, pre-arb |
| | `kalshi_poly_merged_sorted_for_arb.csv` | both venues interleaved (matcher input) |
| | `merged_sorted_for_arb.csv` | same content, short name |
| `4_arbitrage/` | `arbitrage_final.csv` | one row per matched arb pair, with Wendy's running portfolio columns |
| | `matched_trades.csv` | every trade the matcher consumed |
| | `results.txt` | per-venue P&L and the Net |
| `5_charts/` | `charts.html`, `prices.png`, `positions.png`, `locked_in.png` | the visualizations |

A machine-readable index of the last run lands at
`events/last_run_manifest.json`.

## Pipeline

| `arb.py` stage | module | does |
|---|---|---|
| `resolve` | `arbpipe.resolve` | market ids -> `events/<slug>/event.json` |
| `fetch-kalshi` | `arbpipe.fetch.kalshi` | Kalshi historical trades -> `1_raw/kalshi/` |
| `fetch-poly` | `arbpipe.fetch.polymarket_dataapi` | Polymarket trades (v2 API) -> `1_raw/polymarket/` |
| `clean-kalshi` | `arbpipe.clean.kalshi` | -> `2_clean/kalshi/` (shared cross-venue schema) |
| `clean-poly` | `arbpipe.clean.polymarket_dataapi` | -> `2_clean/polymarket/` (same schema) |
| `validate` | `arbpipe.verify.validate` | reconcile both tapes against independent sources |
| `merge` | `arbpipe.arbitrage.merge` | -> `3_sorted/` (the 4 sorted-for-arb CSVs) |
| `match` | `arbpipe.arbitrage.match` | -> `4_arbitrage/arbitrage_final.csv` + `matched_trades.csv` |
| `summary` | `arbpipe.arbitrage.summary` | -> `4_arbitrage/results.txt` |
| `charts` | `arbpipe.report.charts` | -> `5_charts/charts.html` + 3 PNGs |
| `sync` | `arbpipe.report.sync` | cross-venue lead-lag / sync timing (diagnostic) |
| `compare` | `arbpipe.verify.compare` | two events, same market pair, different sources |
| `parity` | `arbpipe.verify.parity` | byte-compare with Wendy's run, or diff our data against hers |
| `run` | `arbpipe.cli` | runs all of the above in order |

Two more are not part of a run: `fetch-poly-rpc` / `clean-poly-rpc`
(`arbpipe.fetch.polymarket_rpc`) read Polymarket from the chain via RPC as a
fallback source, and `reference-match` (`arbpipe.verify.reference_matcher`)
re-demonstrates that our matcher reproduces the reference run.

`compare_runs.py` exists for the re-collections forced by the V2 migration:

```bash
python arb.py compare presidential-2024 presidential-2024-v2
```

It checks the two events' configs match first (a differing `start_ts` alone can
move the Net by double digits), then compares share volume — the invariant that
survives a source change — and only then the Net. **Row counts are expected to
differ ~2x and mean nothing on their own**; a share-volume gap above 1% means a
tape is incomplete rather than merely coarser. Exits non-zero on disagreement,
so it can gate a re-collection.

Stage by stage, if you want to drive it manually:

```bash
python arb.py resolve --kalshi <TICKER> --poly <url|slug|0x…>
python arb.py fetch-kalshi  <slug>
python arb.py clean-kalshi  <slug>
python arb.py fetch-poly    <slug>
python arb.py clean-poly    <slug>
python arb.py validate      <slug>
python arb.py merge         <slug>
python arb.py match         <slug>
python arb.py summary       <slug>
python arb.py charts        <slug>
```

Each is an ordinary module, so `cd src && python -m arbpipe.fetch.kalshi
<slug>` is equivalent — that is exactly what the orchestrator runs
internally.

### Deprecated Polymarket paths

These live in **`src/arbpipe/legacy/`** and have their own README there.
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
arb.py                  single entry point — run this
pyproject.toml          package metadata, optional extras, pytest config

src/arbpipe/            the pipeline, as an importable package
  cli.py                orchestrator: runs every stage in order
  eventlib.py           event config + EVERY output path
  portfolio.py          the (y, z) cash/position model
  resolve.py            market lookup -> event.json
  fetch/                kalshi.py  polymarket_dataapi.py  polymarket_rpc.py
  clean/                kalshi.py  polymarket_dataapi.py  polymarket_rpc.py
  arbitrage/            merge.py  match.py  summary.py
  report/               charts.py  sync.py  charts_template.html
  verify/               validate.py  compare.py  reference_matcher.py  parity.py
  legacy/               DEAD sources, kept as provenance (see its README)

scripts/                shell helpers for the sharded RPC/subgraph runs
tests/                  pytest suite
docs/                   arbitrage matching methodology
notes/                  data dictionary, V2 migration, validation, sync timing

events/<slug>/
  event.json      market ids, exchange, window, resolution, notes  [versioned]
  1_raw/kalshi/   1_raw/polymarket/     verbatim records           [gitignored]
  2_clean/kalshi/ 2_clean/polymarket/   trade-level CSVs           [gitignored]
  3_sorted/       per-venue + merged matcher input                 [versioned]
  4_arbitrage/    arb pairs, matched trades, results.txt           [versioned]
  5_charts/       charts.html + PNGs                               [versioned]
  reference/      the reference run's outputs, to compare against  [versioned]

wendysResults/mamdani-dem-nomination/   Wendy's run, same layout (see its README)
```

Why the `src/` layout: it keeps importable code out of the project root, so
`events/`, `docs/` and `notes/` can never shadow a module. `arb.py` adds
`src/` to `sys.path`, so no install step is needed; `pip install -e .` also
works and then gives you an `arbpipe` console command.

`1_raw/` and `2_clean/` are gitignored (too large) but **fully regenerable** from
`event.json` via the fetch + clean stages. The presidential `3_sorted/` files
are likewise regenerable and not versioned, and no event versions the
byte-identical `kalshi_poly_merged_sorted_for_arb.csv` twin.

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
