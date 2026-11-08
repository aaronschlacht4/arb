# Wendy's run — Mamdani, Democratic primary for NYC Mayor

Market: *Will Zohran Mamdani win the Democratic Primary for Mayor of New York City?*
Kalshi `KXMAYORNYCNOMD-25-ZM`. Our matching event is `events/mamdani-dem-nomination/`,
which uses the same folder layout, so the two can be compared side by side.

Window: 2025-05-27 00:00 ET to 2025-06-24 00:00 ET. Resolution: YES.

## Layout

| Folder | What is in it |
|---|---|
| `1_raw/kalshi/` | `KXMAYORNYCNOMD-25-ZM.json`: every Kalshi trade, as returned by the API (20,177). |
| `1_raw/polymarket/onchain/` | **The Polymarket input.** `…_onchain_orderfilled_all_final.csv`: every `OrderFilled` log for the market (49,936), raw and normalized amounts. `…_poly_transactionHash.csv`: the same logs, raw amounts only. `…_progress_final.txt`: last block scanned. |
| `1_raw/polymarket/subgraph_legacy/` | An older collection from the retired Goldsky subgraph (62,350 rows). Not used by the current run. |
| `2_clean/kalshi/` | `KXMAYORNYCNOMD-25-ZM.csv`: the Kalshi trades flattened to one row each. |
| `2_clean/polymarket/onchain/` | `…_poly_fromHash.csv`: all 49,936 logs decoded to outcome, side, shares, price. `…_poly_takers_only_final.csv`: the 33,533 maker fills left after dropping the aggregate taker logs. **This feeds `3_sorted/`.** |
| `2_clean/polymarket/subgraph_legacy/` | The legacy collection decoded (`fromHash_1`) and its maker fills (`taker_only_1`, 40,823). |
| `3_sorted/` | The matcher's input, cut to the window: `kalshi_sorted_for_arb.csv`, `poly_sorted_for_arb.csv`, and both interleaved in `kalshi_poly_merged_sorted_for_arb.csv`. Identical for every window length. |
| `4_arbitrage/1min/`, `5min/`, `10min/` | One run per matching window: `arbitrage_final_<w>.csv` (one row per matched pair, with running portfolio columns) and `matched_trades_<w>.csv` (every trade consumed). |
| `5_charts/` | `primary_price.png`, `primary_value.png`. **These come from an older run** whose window starts in early April 2025, so they do not match the CSVs above. |
| `_archive/` | Files that are duplicates or superseded versions, kept under their original paths. See below. |

Locked-in value at the end of each run: 1 min $13,504.24, 5 min $25,253.49,
10 min $31,779.50. Our pipeline reproduces every CSV in `3_sorted/` and
`4_arbitrage/` byte for byte from the files in `1_raw/`:

```bash
python arb.py parity mamdani-dem-nomination --her wendysResults/mamdani-dem-nomination --her-raw
```

## `_archive/`

Nothing in here is needed. Exact duplicates are safe to delete.

| File | Why it is archived |
|---|---|
| `kalshi_sorted_for_arb.csv`, `poly_sorted_for_arb.csv`, `kalshi_poly_merged_sorted_for_arb.csv` (top level, `check/`, `result/1min/`, `result/10min/`) | Exact duplicates of `3_sorted/`. |
| `matched_trades.csv` | Exact duplicate of `4_arbitrage/5min/matched_trades_5min.csv`. |
| `matched_trades_1min.csv`, `check/matched_trades_1min_snippet.csv` | Exact duplicates of `4_arbitrage/1min/matched_trades_1min.csv`. |
| `arbitrage_final_1min.csv` | Exact duplicate of `4_arbitrage/1min/arbitrage_final_1min.csv`. |
| `arbitrage_final.csv` | Older 11-column version of the 5 min run. Same matches as `4_arbitrage/5min/`. |
| `check/arbitrage_final_1min_snippet.csv` | Older 9-column version of the 1 min run. Same matches. |
| `Polymarket/mamdani_dem_primary_poly_takers_only.csv` | Same rows as `…_takers_only_final.csv`, in reverse order. |
| `Polymarket/dem_nyc_primary_zm_poly_fromHash.csv`, `…_taker_only.csv` | The legacy `_1` files without their `id` column. |
