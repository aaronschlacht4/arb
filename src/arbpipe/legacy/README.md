# Legacy: data sources that no longer work

**Nothing in this directory can run.** It is kept because it produced the
pre-migration baseline tapes that several events still carry, so it is the
provenance for those numbers — not because it is a fallback.

## Why it is dead

Polymarket migrated to V2. Both of the paths these scripts use were retired,
verified live on 2026-11-04:

| path | what it returns now |
|---|---|
| Goldsky orderbook subgraph | `ENDPOINT_DEPRECATED`, HTTP 429, no data — and the response says the data is "stale and incorrect" even where it answers |
| Data API **v1** | HTTP 410, `{"error": "Data API v1 has been retired"}` |

The replacement is Data API v2: `arbpipe.fetch.polymarket_dataapi` and
`arbpipe.clean.polymarket_dataapi`. See `notes/polymarket_v2_migration.md` for
the full migration record, including the four traps v2 sets.

## What is here

| file | what it did |
|---|---|
| `fetch_polymarket_subgraph.py` | paged the orderbook subgraph, sharded by time range |
| `clean_polymarket_subgraph.py` | undid three artifacts of the maker-fill feed: dropped aggregate `taker == exchange` rows, converted 6-decimal USDC `size` to shares, and flipped the maker-side `side` |
| `combine_subgraph_parts.py` | merged `--suffix .partN` shard files into one raw tape |

## Which events still depend on them

These events' Polymarket tapes came from the subgraph and cannot be
regenerated from this source:

- `popvote-2024`, `presidential-2024` — complete baselines, each re-collected
  from v2 as a `-v2` sibling event
- `popvote-2024-biden`, `popvote-2024-kamala` — deliberate bounded slices
  (2024-06-01..2024-09-01) collected only to timestamp the Biden→Harris switch

## If you need one of these tapes again

Re-collect from v2 into a **new** event slug and compare, rather than editing
the baseline:

```
python arb.py run <new-slug>
python arb.py compare <baseline-slug> <new-slug>
```

Row granularity differs by design — the subgraph gave one row per matched maker
order, v2 gives one per taker order. **Share volume is the invariant**, not the
row count; `arbpipe.verify.compare` checks exactly that.

For markets that stopped trading before roughly Nov 2023, v2's ~3-year window
excludes them and there is no working source in this repo at all.
