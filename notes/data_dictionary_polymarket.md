# Data dictionary — Polymarket clean trades

Market: "Will Zohran Mamdani win the 2025 NYC mayoral election?"
conditionId: `0xebddfcf7b4401dade8b4031770a1ab942b01854f3bed453d5df9425cd9f211a9`

- **Raw**: `data/raw/polymarket_rpc_orderfilled_<conditionId>.jsonl` — one `OrderFilled`
  log per line, verbatim from the chain (via Alchemy `eth_getLogs`). Never modified.
- **Clean**: `data/clean/polymarket_rpc_trades_<conditionId>.csv` — produced by
  `src/clean_polymarket_rpc.py`. One row per **maker-order fill**.

## Row selection (the key transformation)

Each economic trade emits several `OrderFilled` logs: one per matched **maker
order** (topic `taker` = the real taker) plus one **aggregate** taker-order log
(topic `taker` = the Exchange `0xc5d5…f80a`). The aggregate = the SUM of its
maker fills. The clean tape **keeps maker fills, drops the aggregate** to avoid
double-counting. Verified against the Goldsky subgraph (it keeps both; its
`tradesQuantity` counts all logs — used only for completeness validation).

## Shared cross-venue schema (clean tapes)

ALL clean tapes — `polymarket_rpc_trades`, `polymarket_subgraph_trades`, and
`kalshi_trades` — use the SAME columns, so they can be stacked/compared directly.
Venue-specific columns are blank where they don't apply. `clean_kalshi_trades.py`
and `clean_polymarket_*.py` must keep `FIELDS` identical.

| Column | Meaning (identical across venues) | Polymarket | Kalshi |
|--------|-----------------------------------|------------|--------|
| `venue` | source venue | `polymarket` | `kalshi` |
| `timestamp` | trade time, ISO-8601 UTC | block time | `created_time` |
| `unix_ts` | seconds since epoch | from block time | from `created_time` |
| `trade_id` | unique trade id | `<tx_hash>:<log_index>` (rpc) / `<tx>_<orderHash>` (subgraph) | Kalshi `trade_id` |
| `outcome` | contract that traded; **YES = the event happens** (Mamdani/Dem wins) | YES/NO token | `taker_outcome_side` |
| `side` | **taker (aggressor)** direction on `outcome` | maker-side flipped to taker: BUY/SELL | **always `BUY`** — see note below |
| `price` | 0–1, price of the traded `outcome` contract | USDC/token | `yes_price` or `no_price` |
| `quantity` | shares (PM) / contracts (Kalshi) | rpc: token amt ÷1e6 · **subgraph: `(size÷1e6) ÷ price`** | `count_fp` |
| `notional_usd` | USD that changed hands | rpc: USDC amt ÷1e6 · **subgraph: `size ÷ 1e6`** | `price × quantity` |
| `yes_price` | **0–1, canonical implied P(event)** — the cross-venue compare column | `price` if YES else `1−price` | `yes_price_dollars` |
| `tx_hash` | on-chain tx (PM only) | `transactionHash` | *(blank)* |
| `maker` | resting-order owner (PM only) | topic[2] addr | *(blank)* |
| `taker` | aggressor (PM only; never the Exchange) | topic[3] addr | *(blank)* |
| `is_block_trade` | block-trade flag (Kalshi only) | *(blank)* | `is_block_trade` |

**To compare venues:** use `yes_price` (both are implied P(event), 0–1). `price`
is the literal traded-contract price, which for a NO trade is `1 − yes_price`.

### Subgraph `enrichedOrderFilled.size` is USDC, not shares (corrected 2026-07-12)

**`size` is the COLLATERAL (USDC, 6-dec) leg of the fill — not the share count.**
So `notional_usd = size/1e6` and `quantity = (size/1e6) / price`. The earlier code
read `size` as shares and derived `notional = price × size`, getting *both* columns
wrong (quantity off by a factor of `price`, notional by `price²`).

Established, not assumed:
- `sum(size)/1e6` over all rows for a token reproduces the subgraph's own
  `orderbook.scaledCollateralVolume` at ratio **1.000** (YES $54,352,060; NO
  $54,252,147) — i.e. it is collateral.
- On-chain check via the raw `orderFilledEvent` entity, tx `0x294d4e57…84fc`:
  a fill where the maker **gave USDC** ($870 ↔ 3,000 shares @0.29) has enriched
  `size` = 870,000,000; a fill in the same tx where the maker **gave tokens**
  (500 shares ↔ $355 @0.71) has enriched `size` = 355,000,000 — *still* the USDC
  leg. So `size` is always collateral, whichever asset the maker supplied.
- Reconstructed shares match Polymarket's public data-api exactly (355/0.71 = 500).

Sanity anchor: the corrected clean tape for the Trump popular-vote market sums to
**$54.7M** of maker-fill USDC volume, ≈ half of the $108.6M `scaledCollateralVolume`
(which counts the dropped aggregate rows too).

**`side` on a maker-fill row is the MAKER's side** — flip it to get our taker
(aggressor) convention. Verified on 434 trades matched to the public data-api by
`(tx, token)`: flipped agrees 434/434, un-flipped 0/434.

This affects the **subgraph** cleaner only. `clean_polymarket_rpc.py` reads the
asset ids straight from the log and was already correct.

### Why Kalshi `side` is always `BUY` (corrected 2026-07-12)

Kalshi's public trade feed **has no buy/sell field and cannot have one**. Per the
API docs, `taker_outcome_side` is the outcome the taker is *positioned for* —
*"buy-yes and sell-no produce 'yes'; buy-no and sell-yes produce 'no'"* — so the
two directions are folded into one record. And `taker_book_side` is merely a
restatement of it: *"'bid' is equivalent to taker_outcome_side 'yes'; 'ask' is
equivalent to taker_outcome_side 'no'."*

Confirmed empirically: across `POPVOTE-24-R`, `POPVOTE-24-D`, `PRES-2024-DJT` and
both `KXMAYORNYCPARTY-25` markets, only the pairs `(bid,yes)` and `(ask,no)` ever
occur — never `(ask,yes)` or `(bid,no)`. The fields are perfectly collinear.

The previous rule (`ask→BUY, bid→SELL`) therefore recovered **no** direction: it
silently re-encoded `outcome`, tagging every YES trade "SELL" and every NO trade
"BUY". Corrected to: the Kalshi taker always **acquires** `outcome`, so `side` is
always `BUY` and `outcome` carries the direction. A Polymarket "SELL YES" is the
same economic event as a Kalshi "BUY NO".

This never affected `price`, `quantity`, `yes_price`, or the arbitrage results —
`merge_for_arb.py` derives its own side from `outcome`, not from this column.

## Conventions
- USDC assetId = `0`; all on-chain amounts are 6-decimal integers (÷1e6).
- Kalshi market `KXMAYORNYCPARTY-25-D` resolves on **party** (a Democrat wins);
  Polymarket resolves on **Mamdani** specifically — closely related, not identical.

## Validation hooks (see notes/polymarket_validation.md)
- Completeness: raw log count per token == subgraph `orderbook.tradesQuantity`
  (YES 398,505 / NO 190,104).
- Correctness: per-tx spot-check of price/notional_usd/side vs subgraph
  `enrichedOrderFilled` (confirmed exact on tx 0x3a4ab7…84a7).
