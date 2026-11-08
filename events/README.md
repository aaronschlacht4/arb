# events/

One folder per market pair (the same real-world question listed on Kalshi and
Polymarket). Every folder has the same layout, numbered in the order the data
flows through the pipeline, with the two venues side by side:

```
<slug>/
  event.json                 market ids, window, resolution: every stage reads this
  1_raw/
    kalshi/                  trades exactly as the Kalshi API returned them
    polymarket/              trades exactly as Polymarket returned them
  2_clean/
    kalshi/                  one trade per row, shared cross-venue schema
    polymarket/              same schema
  3_sorted/
    kalshi_sorted_for_arb.csv               Kalshi tape, windowed and sorted
    poly_sorted_for_arb.csv                 Polymarket tape, windowed and sorted
    merged_sorted_for_arb.csv               both interleaved: the matcher's input
    kalshi_poly_merged_sorted_for_arb.csv   same file under Wendy's name
  4_arbitrage/
    arbitrage_final.csv      one row per matched arb pair, with running portfolio
    matched_trades.csv       every trade the matcher consumed
    results.txt              per-venue P&L and the Net
  5_charts/
    charts.html  prices.png  positions.png  locked_in.png
  reference/                 the original study's files, where we have them
```

`1_raw/` and `2_clean/` are not in git: they are large and regenerate from
`event.json` with `python arb.py run <slug>`. Folders 3 to 5 are versioned.
A folder that is missing has simply not been produced in this clone.

Wendy's run of the Mamdani primary uses the same layout, in
`wendysResults/mamdani-dem-nomination/`.

## The events

| Folder | Kalshi | Question | Net | In this clone |
|---|---|---|---|---|
| `mamdani-dem-nomination` | `KXMAYORNYCNOMD-25-ZM` | Mamdani wins the Democratic primary | $25,508 | everything, 1 to 5 |
| `mamdani-nyc-mayor` | `KXMAYORNYCPARTY-25-D` | Mamdani wins NYC mayor | $390,993 | 3 to 5 |
| `popvote-2024` | `POPVOTE-24-R` | Trump wins the popular vote (subgraph data) | $490,526 | 3 to 5 |
| `popvote-2024-v2` | `POPVOTE-24-R` | same market, re-collected from the Data API | $490,150 | 3 to 5 |
| `presidential-2024` | `PRES-2024-DJT` | Trump wins the presidency (subgraph data) | $3,049,168 | 4 and 5 |
| `presidential-2024-v2` | `PRES-2024-DJT` | same market, re-collected from the Data API | $3,049,253 | 4 and 5 |
| `popvote-2024-biden` | `POPVOTE-24-D` | Biden wins the popular vote: time slice only | none | config only |
| `popvote-2024-kamala` | `POPVOTE-24-D` | Harris wins the popular vote: time slice only | none | config only |

`last_run_manifest.json` lists the files the most recent `run` produced.
