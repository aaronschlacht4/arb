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
| `side` | **taker (aggressor)** BUY/SELL of `outcome` | maker-side flipped to taker | `taker_book_side`: ask→BUY, bid→SELL |
| `price` | 0–1, price of the traded `outcome` contract | USDC/token | `yes_price` or `no_price` |
| `quantity` | shares (PM) / contracts (Kalshi) | token amt ÷1e6 | `count_fp` |
| `notional_usd` | `price × quantity` (USD changed hands) | = USDC amount ÷1e6 | computed |
| `yes_price` | **0–1, canonical implied P(event)** — the cross-venue compare column | `price` if YES else `1−price` | `yes_price_dollars` |
| `tx_hash` | on-chain tx (PM only) | `transactionHash` | *(blank)* |
| `maker` | resting-order owner (PM only) | topic[2] addr | *(blank)* |
| `taker` | aggressor (PM only; never the Exchange) | topic[3] addr | *(blank)* |
| `is_block_trade` | block-trade flag (Kalshi only) | *(blank)* | `is_block_trade` |

**To compare venues:** use `yes_price` (both are implied P(event), 0–1). `price`
is the literal traded-contract price, which for a NO trade is `1 − yes_price`.

## Conventions
- USDC assetId = `0`; all on-chain amounts are 6-decimal integers (÷1e6).
- Kalshi market `KXMAYORNYCPARTY-25-D` resolves on **party** (a Democrat wins);
  Polymarket resolves on **Mamdani** specifically — closely related, not identical.

## Validation hooks (see notes/polymarket_validation.md)
- Completeness: raw log count per token == subgraph `orderbook.tradesQuantity`
  (YES 398,505 / NO 190,104).
- Correctness: per-tx spot-check of price/notional_usd/side vs subgraph
  `enrichedOrderFilled` (confirmed exact on tx 0x3a4ab7…84a7).
