# arb — cross-venue prediction-market data & arbitrage

Trade-level data collection for the same real-world event listed on **Kalshi** and
**Polymarket**, and a reproduction of the two-pointer arbitrage matcher used in
the original study.

Everything is **event-driven**: one folder per market pair under `events/<slug>/`,
one `event.json` that every stage reads. The same code runs on any event.

---

## Events

| slug | Kalshi | Polymarket | Net (arb) |
|---|---|---|---|
| `presidential-2024` | `PRES-2024-DJT` | Trump wins the presidency | **$3,049,168** |
| `popvote-2024` | `POPVOTE-24-R` | Trump wins the popular vote | **$490,526** |
| `mamdani-nyc-mayor` | `KXMAYORNYCPARTY-25-D` | Mamdani wins NYC mayor | $392,592 |
| `mamdani-dem-nomination` | `KXMAYORNYCNOMD-25-ZM` | Mamdani wins the primary | $25,934 |
| `popvote-2024-biden` | *(none in window)* | Biden wins the popular vote | — |
| `popvote-2024-kamala` | *(none in window)* | Harris wins the popular vote | — |

The two `popvote-2024-{biden,kamala}` events are **deliberate time slices**
(2024-06-01 → 2024-09-01), collected only to timestamp the Biden→Harris switch.
They are not complete market histories — see their `event.json`.

## Pipeline

```
fetch_kalshi_trades.py        Kalshi historical trades    -> raw/*.jsonl
fetch_polymarket_subgraph.py  Polymarket fills (subgraph) -> raw/*.part<N>.jsonl[.gz]
combine_subgraph_parts.py     merge the shards            -> raw/*.jsonl[.gz]
clean_kalshi_trades.py        raw -> clean/*.csv   (shared cross-venue schema)
clean_polymarket_subgraph.py  raw -> clean/*.csv   (same schema)
validate_event.py             reconcile both tapes against independent sources
merge_for_arb.py              clean tapes -> results/merged_sorted_for_arb.csv
run_arbitrage.py              -> results/arbitrage_final.csv + matched_trades.csv
results_summary.py            -> results/results.txt
make_charts.py                -> results/charts.html + 3 PNGs
analyze_sync.py               cross-venue lead-lag / sync timing (diagnostic)
```

Typical run for a new event:

```bash
python src/fetch_kalshi_trades.py        <slug>
python src/clean_kalshi_trades.py        <slug>
# Polymarket: shard by time, run the shards in parallel, then combine
python src/fetch_polymarket_subgraph.py  <slug> --start-ts A --end-ts B --suffix .part1
python src/combine_subgraph_parts.py     <slug>
python src/clean_polymarket_subgraph.py  <slug>
python src/validate_event.py             <slug>
python src/merge_for_arb.py              <slug>
python src/run_arbitrage.py              <slug>
python src/results_summary.py            <slug>
python src/make_charts.py                <slug>
```

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

Polymarket is collected from the Goldsky orderbook subgraph (no API key). Each tape
is reconciled three ways: raw fill count vs the indexer's own
`orderbook.tradesQuantity`; every trade in Polymarket's public data-api present in
our raw; and our reconstructed share total vs Polymarket's published volume.

On `presidential-2024` the subgraph-derived arbitrage reproduces the earlier
RPC-derived run to **0.024%** on Net, so the missing on-chain `logIndex` — which
only affects within-second consumption order — is immaterial.

An Alchemy RPC path (`fetch_polymarket_rpc.py`) also exists and is the only source
of a true `logIndex`; it needs `ALCHEMY_POLYGON_URL` in `.env`.
