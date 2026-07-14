"""
Validate an event's CLEAN tapes against independent ground truth.

The cleaning scripts print their own summaries, but those are self-reported: if
the fetch silently truncated, the cleaner happily reports a smaller number and
everything "looks fine". This script exists to catch exactly that.

Checks per venue:
  STRUCTURE  duplicate trade ids, timestamp ordering, price/yes_price in (0,1),
             price == 1 - yes_price for NO rows, non-positive quantities.
  COMPLETENESS
    Polymarket — reconcile against the subgraph's OWN aggregate counters
      (`orderbook.tradesQuantity` / `scaledCollateralVolume`), which are computed
      by the indexer independently of the fills we paged. This is the check that
      catches a truncated scan.
      NOTE: tradesQuantity counts EVERY enrichedOrderFilled row (maker fills AND
      the aggregate taker rows we drop), so the raw line count — not the clean
      row count — is what must match it.
    Kalshi — re-request the newest page from the API and confirm our newest trade
      matches, i.e. we paged all the way to the end of the tape.
  OVERLAP    the window where both venues traded (the only period comparable
             across venues) and their mean implied probability in it.

Run:
    python src/validate_event.py popvote-2024
"""

import csv
import json
import sys
import urllib.request
from collections import Counter

from eventlib import load_event, open_raw
from fetch_polymarket_subgraph import load_endpoint, HEADERS

KALSHI_TRADES = "https://api.elections.kalshi.com/trade-api/v2/historical/trades"

ORDERBOOK_QUERY = """
query($id: ID!) {
  orderbook(id: $id) {
    id
    tradesQuantity
    buysQuantity
    sellsQuantity
    scaledCollateralVolume
  }
}
"""

OK, BAD, WARN = "  [ok]  ", "  [FAIL]", "  [warn]"


def read_csv(path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def check_structure(rows: list[dict], label: str) -> list[str]:
    """Venue-agnostic sanity checks on a clean tape. Returns failure strings."""
    fails = []
    ids = [r["trade_id"] for r in rows]
    dups = len(ids) - len(set(ids))
    print(f"{OK if not dups else BAD} {label}: duplicate trade_ids = {dups}")
    if dups:
        fails.append(f"{label}: {dups} duplicate trade_ids")

    ts = [int(r["unix_ts"]) for r in rows]
    sorted_ok = all(ts[i] <= ts[i + 1] for i in range(len(ts) - 1))
    print(f"{OK if sorted_ok else BAD} {label}: timestamp ordering")
    if not sorted_ok:
        fails.append(f"{label}: rows not sorted by time")

    bad_px = [r for r in rows if not 0.0 < float(r["price"]) < 1.0]
    bad_yes = [r for r in rows if not 0.0 < float(r["yes_price"]) < 1.0]
    print(f"{OK if not bad_px else BAD} {label}: price in (0,1) — "
          f"{len(bad_px)} violations")
    print(f"{OK if not bad_yes else BAD} {label}: yes_price in (0,1) — "
          f"{len(bad_yes)} violations")
    if bad_px:
        fails.append(f"{label}: {len(bad_px)} prices outside (0,1)")
    if bad_yes:
        fails.append(f"{label}: {len(bad_yes)} yes_prices outside (0,1)")

    # For a NO trade, the traded contract's price must be the complement of the
    # implied P(event). If this breaks, the outcome->price mapping is wrong.
    off = [r for r in rows if r["outcome"] == "NO"
           and abs((1 - float(r["yes_price"])) - float(r["price"])) > 1e-6]
    print(f"{OK if not off else BAD} {label}: NO rows satisfy "
          f"price == 1 - yes_price — {len(off)} violations")
    if off:
        fails.append(f"{label}: {len(off)} NO rows where price != 1-yes_price")

    nonpos = [r for r in rows if float(r["quantity"]) <= 0]
    print(f"{OK if not nonpos else BAD} {label}: quantity > 0 — "
          f"{len(nonpos)} violations")
    if nonpos:
        fails.append(f"{label}: {len(nonpos)} non-positive quantities")
    return fails


def gql_orderbook(url: str, token_id: str) -> dict:
    body = json.dumps({"query": ORDERBOOK_QUERY,
                       "variables": {"id": token_id}}).encode()
    req = urllib.request.Request(url, data=body, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))["data"]["orderbook"]


def validate_polymarket(ev) -> list[str]:
    fails = []
    raw_path, clean_path = ev.poly_subgraph_raw_jsonl, ev.poly_subgraph_clean_csv
    if not clean_path.exists():
        print(f"{WARN} polymarket: no clean tape, skipping")
        return fails

    print("\n--- POLYMARKET ---")
    rows = read_csv(clean_path)
    fails += check_structure(rows, "polymarket")

    # Completeness: our RAW line count per token vs the subgraph's own counter.
    raw_per_token = Counter()
    with open_raw(raw_path, "rt", ev.poly_raw_gzip) as f:
        for line in f:
            raw_per_token[json.loads(line)["market"]] += 1

    url = load_endpoint()
    for name, tok in zip(("YES", "NO"), ev.poly_token_ids):
        ob = gql_orderbook(url, tok)
        expected = int(ob["tradesQuantity"])
        got = raw_per_token[tok]
        ok = got == expected
        print(f"{OK if ok else BAD} polymarket {name}: raw fills {got:,} vs "
              f"subgraph tradesQuantity {expected:,}"
              + ("" if ok else f"  (MISSING {expected - got:,})"))
        if not ok:
            fails.append(f"polymarket {name}: raw {got:,} != subgraph "
                         f"{expected:,} (incomplete fetch)")

    vol = sum(float(r["notional_usd"]) for r in rows)
    print(f"         clean trades: {len(rows):,}   USDC volume: ${vol:,.0f}")
    print(f"         date range  : {rows[0]['timestamp'][:10]} .. "
          f"{rows[-1]['timestamp'][:10]}")
    print(f"         by outcome  : {dict(Counter(r['outcome'] for r in rows))}")
    return fails


def validate_kalshi(ev) -> list[str]:
    fails = []
    clean_path = ev.kalshi_clean_csv
    if not clean_path.exists():
        print(f"{WARN} kalshi: no clean tape, skipping")
        return fails

    print("\n--- KALSHI ---")
    rows = read_csv(clean_path)
    fails += check_structure(rows, "kalshi")

    # Completeness: the API returns trades newest-first, so page 1 holds the most
    # recent trade. If our tape's newest trade is that trade, we paged to the end.
    url = f"{KALSHI_TRADES}?ticker={ev.kalshi_ticker}&limit=1"
    req = urllib.request.Request(url, headers={"User-Agent": "arb-research/0.1"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        newest = json.loads(resp.read().decode("utf-8"))["trades"][0]
    ours = rows[-1]["trade_id"]
    ok = ours == newest["trade_id"]
    print(f"{OK if ok else BAD} kalshi: newest trade matches API head "
          f"({newest['trade_id'][:8]}…)")
    if not ok:
        fails.append(f"kalshi: newest clean trade {ours[:8]}… != API head "
                     f"{newest['trade_id'][:8]}… (tape may be truncated)")

    # side must be BUY everywhere — Kalshi exposes no buy/sell field (see
    # clean_kalshi_trades.py docstring). Anything else means the old bug is back.
    sides = set(r["side"] for r in rows)
    ok = sides == {"BUY"}
    print(f"{OK if ok else BAD} kalshi: side is always BUY — found {sides}")
    if not ok:
        fails.append(f"kalshi: unexpected side values {sides}")

    contracts = sum(float(r["quantity"]) for r in rows)
    notional = sum(float(r["notional_usd"]) for r in rows)
    print(f"         clean trades: {len(rows):,}   contracts: {contracts:,.0f}   "
          f"notional: ${notional:,.0f}")
    print(f"         date range  : {rows[0]['timestamp'][:10]} .. "
          f"{rows[-1]['timestamp'][:10]}")
    print(f"         by outcome  : {dict(Counter(r['outcome'] for r in rows))}")
    return fails


def report_overlap(ev) -> None:
    """The only window where the two venues are actually comparable."""
    if not (ev.kalshi_clean_csv.exists() and ev.poly_subgraph_clean_csv.exists()):
        return
    k = read_csv(ev.kalshi_clean_csv)
    p = read_csv(ev.poly_subgraph_clean_csv)
    lo = max(int(k[0]["unix_ts"]), int(p[0]["unix_ts"]))
    hi = min(int(k[-1]["unix_ts"]), int(p[-1]["unix_ts"]))
    if lo >= hi:
        print("\n--- OVERLAP ---\n  none (the venues never traded concurrently)")
        return

    kk = [r for r in k if lo <= int(r["unix_ts"]) <= hi]
    pp = [r for r in p if lo <= int(r["unix_ts"]) <= hi]
    from datetime import datetime, timezone
    from statistics import median
    fmt = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")

    # Compare DAILY means, not a pooled mean over trades. A pooled mean is
    # composition-weighted: whichever venue happens to trade more heavily on
    # high-price days looks systematically different even when the two price
    # paths are on top of each other. (On this event a pooled mean showed a
    # 0.13 "gap" where the real daily gap is ~0.03.)
    def by_day(rows):
        d = {}
        for r in rows:
            d.setdefault(r["timestamp"][:10], []).append(float(r["yes_price"]))
        return {k: sum(v) / len(v) for k, v in d.items()}

    kd, pd_ = by_day(kk), by_day(pp)
    days = sorted(set(kd) & set(pd_))
    gaps = [kd[d] - pd_[d] for d in days]

    print("\n--- OVERLAP (both venues trading) ---")
    print(f"         window        : {fmt(lo)} .. {fmt(hi)}  ({len(days)} days)")
    print(f"         kalshi        : {len(kk):,} trades")
    print(f"         polymarket    : {len(pp):,} trades")
    print(f"         daily yes_price gap (kalshi - polymarket):")
    print(f"           median      : {median(gaps):+.4f}")
    print(f"           min / max   : {min(gaps):+.4f} / {max(gaps):+.4f}")
    print(f"           days k > p  : {sum(1 for g in gaps if g > 0)}/{len(days)}")
    print("         NOTE: a persistent one-sided gap is BASIS, not free money —"
          " these are\n               not the same contract (see event.json).")


def main() -> None:
    ev = load_event(sys.argv[1] if len(sys.argv) > 1 else None)
    print(f"Validating event: {ev.slug}  ({ev.name})")

    fails = validate_kalshi(ev) + validate_polymarket(ev)
    report_overlap(ev)

    print("\n" + "=" * 62)
    if fails:
        print(f"VALIDATION FAILED — {len(fails)} problem(s):")
        for x in fails:
            print(f"  - {x}")
        sys.exit(1)
    print("VALIDATION PASSED — all checks green.")


if __name__ == "__main__":
    main()
