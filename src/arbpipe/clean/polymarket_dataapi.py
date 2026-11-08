"""
Clean the Data-API-v2 raw Polymarket trades into a trade-level CSV.

INPUT  (raw, never modified): raw/polymarket_dataapi_trades_<conditionId>.jsonl
OUTPUT (derived, overwritable): clean/polymarket_dataapi_trades_<conditionId>.csv

Emits the SHARED cross-venue schema (see notes/data_dictionary_polymarket.md) so
this tape is column-for-column identical to the Kalshi tape and to the older
subgraph/RPC tapes.

WHY THIS IS SIMPLER THAN clean_polymarket_subgraph.py
The subgraph cleaner had to undo three artifacts of the raw orderbook feed. NONE
of them applies to Data API v2, and applying them here would CORRUPT the tape:

  1. NO aggregate rows to drop. The subgraph emitted one row per matched maker
     order PLUS an aggregate taker-order row (taker == the Exchange contract),
     and the cleaner dropped the aggregate to avoid double-counting. The v2
     taker view has no such row, so `polymarket_exchange` is not consulted here
     at all. (Dropping nothing is correct; there is nothing to drop.)

  2. `size` IS SHARES, already decimal — not 6-decimal USDC.
     The subgraph's `size` was the USDC collateral leg scaled by 1e6, so that
     cleaner computes `quantity = (size/1e6)/price`. Here `size` is the share
     count as a plain float (e.g. 78.54) and
         quantity     = size
         notional_usd = size * price
     Carrying the old formula over would divide every quantity by `price` and
     mis-scale notional by 1e6.

  3. `side` IS ALREADY THE TAKER'S. The subgraph reported the MAKER's direction
     on maker-fill rows, so that cleaner flips it. The v2 taker view
     (`taker_only=true`, which the fetcher pins explicitly) reports the taker's
     own direction. Flipping it here would invert every trade's direction.

  Each of these was read off the live API rather than assumed; see
  fetch_polymarket_dataapi.py for the probes.

FIELDS
  outcome   YES/NO  — from the row's own `outcome` ("Yes"/"No"), NOT from
                      `outcome_index` and NOT from token-id position. The row
                      states its own outcome, so we never have to trust an
                      ordering that could silently invert the tape.
  side      BUY/SELL — the taker's direction, verbatim.
  price     0-1     — price of the traded `outcome` contract.
  quantity         — shares (`size`).
  notional_usd     — size * price.
  yes_price 0-1    — canonical implied P(event): price if outcome is YES,
                      else 1 - price. This is the cross-venue comparison column.

TRADE ID. The v2 Trade schema has no `id`. `transaction_hash` was unique across
every row checked (412,000+ rows, zero collisions), but one transaction CAN in
principle carry two taker orders, so we do not rely on it: the id is the tx hash,
plus a `#<n>` suffix only on the 2nd and later rows sharing a hash. That keeps
the common case byte-identical to the tx hash while staying collision-proof.
The suffix is assigned after a deterministic sort, so it is reproducible.

Run:
    python arb.py clean-poly mamdani-nyc-mayor
"""

import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone

from arbpipe.eventlib import load_event, open_raw, raw_path

# Shared cross-venue schema (must stay identical in clean_kalshi_trades.py).
FIELDS = [
    "venue", "timestamp", "unix_ts", "trade_id", "outcome", "side",
    "price", "quantity", "notional_usd", "yes_price",
    "tx_hash", "maker", "taker", "is_block_trade",
]

# Column indices into the row tuples `decode` returns. Named so the code below
# never indexes by a bare integer.
(F_VENUE, F_TS, F_UNIX, F_TID, F_OUTCOME, F_SIDE, F_PRICE, F_QTY,
 F_NOTIONAL, F_YES, F_TX, F_MAKER, F_TAKER, F_BLOCK) = range(len(FIELDS))


def decode(rec: dict, token_name: dict) -> tuple | None:
    """One v2 trade record -> a clean row, or None if it is unusable."""
    ts = int(rec["timestamp"])
    price = round(float(rec["price"]), 6)
    if not 0 < price < 1:
        # A 0 or 1 price carries no probability information and would make
        # yes_price degenerate; such rows are not tradeable quotes.
        return None

    size = float(rec["size"])
    if size <= 0:
        return None

    # Prefer the row's own label; fall back to the token-id map only if absent.
    outcome = (rec.get("outcome") or "").strip().upper()
    if outcome not in ("YES", "NO"):
        outcome = token_name.get(rec.get("token_id"), "")
    if outcome not in ("YES", "NO"):
        return None

    side = (rec.get("side") or "").strip().upper()
    if side not in ("BUY", "SELL"):
        return None

    notional = size * price
    yes_price = price if outcome == "YES" else round(1.0 - price, 6)
    tx = rec.get("transaction_hash") or ""
    # A TUPLE in FIELDS order, not a dict. presidential-2024 is ~1.5M clean
    # rows here and a 14-key dict per row costs several GB, which is what
    # forced the same choice in clean_polymarket_subgraph.py. Index with the
    # F_* constants below rather than by position literal.
    return (
        "polymarket",
        datetime.fromtimestamp(ts, timezone.utc).isoformat(),
        ts,
        tx,                             # trade_id; may gain a #n suffix below
        outcome,
        side,                           # already the taker's direction
        price,
        round(size, 6),
        round(notional, 6),
        yes_price,
        tx,                             # tx_hash
        # v2 gives ONE wallet per trade (the taker's), not a maker/taker pair.
        # Recording the taker and leaving maker blank is honest; inventing a
        # counterparty would not be.
        "",                             # maker
        rec.get("proxy_wallet") or "",  # taker
        "",                             # is_block_trade
    )


def main() -> None:
    ev = load_event(sys.argv[1] if len(sys.argv) > 1 else None)
    token_name = {}
    if len(ev.poly_token_ids) >= 2:
        token_name = {ev.poly_token_ids[0]: "YES", ev.poly_token_ids[1]: "NO"}

    src = raw_path(ev.poly_dataapi_raw_jsonl, ev.poly_raw_gzip)
    if not src.exists():
        raise SystemExit(
            f"Raw file not found: {src}\n"
            f"Run: python arb.py fetch-poly {ev.slug}")

    n_recs = n_skip = n_wrong_market = 0
    rows: list[tuple] = []
    with open_raw(ev.poly_dataapi_raw_jsonl, "rt", ev.poly_raw_gzip) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            n_recs += 1
            rec = json.loads(line)
            # Belt and braces: the fetcher aborts on contamination, but a tape
            # acquired by other means could still mix markets.
            if rec.get("condition_id") and \
                    rec["condition_id"] != ev.poly_condition_id:
                n_wrong_market += 1
                continue
            out = decode(rec, token_name)
            if out is None:
                n_skip += 1
            else:
                rows.append(out)

    if n_wrong_market:
        raise SystemExit(
            f"{n_wrong_market:,} raw rows belong to a different condition id. "
            f"Refusing to write a mixed tape — re-fetch this event's raw file.")

    # Deterministic order first, so the collision suffix below is reproducible.
    rows.sort(key=lambda r: (r[F_UNIX], r[F_TID], r[F_OUTCOME],
                             r[F_SIDE], r[F_PRICE], r[F_QTY]))

    # Disambiguate any transaction that carries more than one taker order. The
    # first keeps the bare tx hash; later ones get #1, #2, … so existing ids
    # are unchanged in the overwhelmingly common 1-order-per-tx case.
    # Tuples are immutable, so a suffixed row is rebuilt rather than mutated —
    # only the handful that collide pay for it.
    seen: Counter = Counter()
    n_suffixed = 0
    for i, r in enumerate(rows):
        tx = r[F_TX]
        k = seen[tx]
        seen[tx] += 1
        if k:
            rows[i] = r[:F_TID] + (f"{tx}#{k}",) + r[F_TID + 1:]
            n_suffixed += 1
    del seen          # a Counter over every tx hash is large; drop it early

    # CHECK BEFORE WRITING. If this fails after the CSV is on disk, the next
    # run skips the clean stage (the output exists) and the matcher consumes a
    # tape with colliding ids, silently mis-attributing consumed size. Verify
    # first; write only what passed.
    ids = {r[F_TID] for r in rows}
    if len(ids) != len(rows):
        raise SystemExit(
            f"trade_id is not unique after suffixing ({len(ids):,} ids for "
            f"{len(rows):,} rows) — the matcher requires unique ids. "
            f"No clean tape written.")
    del ids

    with ev.poly_dataapi_clean_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(FIELDS)
        w.writerows(rows)

    by_outcome = Counter(r[F_OUTCOME] for r in rows)
    by_side = Counter(r[F_SIDE] for r in rows)
    vol = sum(r[F_NOTIONAL] for r in rows)
    shares = sum(r[F_QTY] for r in rows)
    print(f"data-api records read : {n_recs:,}")
    print(f"  unusable (dropped)  : {n_skip:,}")
    print(f"clean trades written  : {len(rows):,}  -> {ev.poly_dataapi_clean_csv}")
    print(f"  by outcome          : {dict(by_outcome)}")
    print(f"  by side             : {dict(by_side)}")
    if n_suffixed:
        print(f"  tx with >1 order    : {n_suffixed:,} rows got a #n suffix")
    if rows:
        print(f"  date range          : {rows[0][F_TS][:10]} .. "
              f"{rows[-1][F_TS][:10]}")
        print(f"  price range         : {min(r[F_PRICE] for r in rows)} .. "
              f"{max(r[F_PRICE] for r in rows)}")
        print(f"  shares              : {shares:,.0f}")
        print(f"  total USDC volume   : ${vol:,.0f}")


if __name__ == "__main__":
    main()
