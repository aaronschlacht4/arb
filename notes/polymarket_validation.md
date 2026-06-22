# Polymarket validation reference (Mamdani NYC-mayor market)

The raw `eth_getLogs` scan (`src/fetch_polymarket_rpc.py`) is the PRIMARY data
source (on-chain `OrderFilled` logs). This subgraph is used ONLY to VALIDATE that
the scan + cleaning produced a complete, correct trade-level dataset.

## Verified working subgraph (Goldsky, public, no auth)

Endpoint:
    https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/polymarket-orderbook-resync/prod/gn

Key facts discovered empirically:
- Filtering `orderFilledEvent` by raw `makerAssetId` (a string column) TIMES OUT
  (not indexed for fast lookup).
- The FAST path is `enrichedOrderFilled`, which has an INDEXED `market` field
  (= the outcome-token id / orderbook id) plus PRE-DECODED `price`, `size`,
  `side`, `timestamp`, `maker`, `taker`, `transactionHash`. ~0.45s per page.
- The `orderbook` entity, keyed by token id, gives instant aggregate truth.

## Validation targets (from `orderbook(id: <tokenId>)`)

| Token | id | tradesQuantity | volume (scaledCollateral) |
|-------|----|----------------|---------------------------|
| YES   | 33945469250963963541781051637999677727672635213493648594066577298999471399137 | 398,505 | 176,445,010.18 |
| NO    | 105832362350788616148612362642992403996714020918558917275151746177525518770551 | 190,104 | (n/a pulled) |

Total trade fills (YES + NO): **588,609**

## Planned validation checks (run AFTER the scan finishes)

1. COUNT: after collapsing the 2 `OrderFilled` logs/trade, our reconstructed
   trade count per token should reconcile with tradesQuantity above. (Mind the
   maker-side vs taker-aggregate-side doubling — enrichedOrderFilled is already
   the deduped maker-order view.)
2. SPOT-CHECK: sample N transactionHashes; compare our decoded price/size/side/
   timestamp against `enrichedOrderFilled` for the same tx.
3. BOUNDARY: confirm our earliest captured trade >= scan start block (market
   didn't trade before 2025-04-22) and latest <= end block.
4. VOLUME: sum(price*size) of our cleaned trades vs scaledCollateralVolume.

## Example fast query

    { enrichedOrderFilleds(first:1000, orderBy:timestamp, orderDirection:asc,
        where:{market:"<tokenId>"}) {
          transactionHash timestamp price size side maker { id } taker { id } } }
