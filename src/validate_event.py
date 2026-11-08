"""
Validate an event's CLEAN tapes against independent ground truth.

The cleaning scripts print their own summaries, but those are self-reported: if
the fetch silently truncated, the cleaner happily reports a smaller number and
everything "looks fine". This script exists to catch exactly that.

Checks per venue:
  STRUCTURE  duplicate trade ids, timestamp ordering, price/yes_price in (0,1),
             price == 1 - yes_price for NO rows, non-positive quantities.
  COMPLETENESS
    Polymarket — two independent checks, because the Data API can truncate
      SILENTLY (its cursor does not carry the market filter, so a paged request
      that drops `condition_id` returns an empty page that looks exactly like
      end-of-data):
        volume — sum(quantity) vs Gamma's `volumeNum`, which Gamma computes from
          its own indexer, in shares. Tolerance 1%.
        head   — our newest trade vs the API's newest trade. A tape that stopped
          early fails this immediately.
      (The old check against the Goldsky subgraph's `orderbook.tradesQuantity`
      is gone: that endpoint was deprecated in Polymarket's V2 migration.)
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

from eventlib import load_event

KALSHI_TRADES = "https://api.elections.kalshi.com/trade-api/v2/historical/trades"
POLY_TRADES = "https://data-api.polymarket.com/v2/trades"

OK, BAD, WARN = "  [ok]  ", "  [FAIL]", "  [warn]"


def read_csv(path) -> list[dict]:
    """Materialize a whole tape. Only safe on tapes known to be small.

    Do NOT use this on a Polymarket or Kalshi clean tape — presidential-2024 is
    ~2.7M rows and a dict per row costs multiple GB. Use `scan()` instead.
    """
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


class Scan:
    """Everything the checks need from a clean tape, gathered in ONE pass.

    The tape is streamed rather than materialized: at presidential-2024 scale a
    list of row dicts is several GB, which is the same trap that forced tuples
    in clean_polymarket_dataapi.py. Memory here is flat except for the trade_id
    set, which is needed for the duplicate check and is just strings.
    """

    def __init__(self):
        self.n = 0
        self.ids: set[str] = set()
        self.dups = 0
        self.unsorted = 0
        self.bad_px = 0
        self.bad_yes = 0
        self.no_mismatch = 0
        self.nonpos = 0
        self.shares = 0.0
        self.notional = 0.0
        self.by_outcome: Counter = Counter()
        self.sides: set[str] = set()
        self.first_ts = self.last_ts = None
        self.first_iso = self.last_iso = None
        self.last_id = None
        self._prev_unix = None

    def feed(self, r: dict) -> None:
        self.n += 1
        tid = r["trade_id"]
        if tid in self.ids:
            self.dups += 1
        else:
            self.ids.add(tid)
        self.last_id = tid

        unix = int(r["unix_ts"])
        if self._prev_unix is not None and unix < self._prev_unix:
            self.unsorted += 1
        self._prev_unix = unix

        px = float(r["price"])
        yes = float(r["yes_price"])
        qty = float(r["quantity"])
        if not 0.0 < px < 1.0:
            self.bad_px += 1
        if not 0.0 < yes < 1.0:
            self.bad_yes += 1
        if r["outcome"] == "NO" and abs((1 - yes) - px) > 1e-6:
            self.no_mismatch += 1
        if qty <= 0:
            self.nonpos += 1

        self.shares += qty
        self.notional += float(r["notional_usd"])
        self.by_outcome[r["outcome"]] += 1
        self.sides.add(r["side"])

        if self.first_ts is None:
            self.first_ts, self.first_iso = unix, r["timestamp"]
        self.last_ts, self.last_iso = unix, r["timestamp"]


def scan(path) -> Scan:
    """Stream a clean tape through the checks in one pass."""
    s = Scan()
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            s.feed(r)
    return s


def check_structure(s: Scan, label: str) -> list[str]:
    """Venue-agnostic sanity checks on a scanned tape. Returns failure strings."""
    fails = []
    print(f"{OK if not s.dups else BAD} {label}: duplicate trade_ids = {s.dups}")
    if s.dups:
        fails.append(f"{label}: {s.dups} duplicate trade_ids")

    print(f"{OK if not s.unsorted else BAD} {label}: timestamp ordering")
    if s.unsorted:
        fails.append(f"{label}: rows not sorted by time ({s.unsorted} inversions)")

    print(f"{OK if not s.bad_px else BAD} {label}: price in (0,1) — "
          f"{s.bad_px} violations")
    print(f"{OK if not s.bad_yes else BAD} {label}: yes_price in (0,1) — "
          f"{s.bad_yes} violations")
    if s.bad_px:
        fails.append(f"{label}: {s.bad_px} prices outside (0,1)")
    if s.bad_yes:
        fails.append(f"{label}: {s.bad_yes} yes_prices outside (0,1)")

    # For a NO trade, the traded contract's price must be the complement of the
    # implied P(event). If this breaks, the outcome->price mapping is wrong.
    print(f"{OK if not s.no_mismatch else BAD} {label}: NO rows satisfy "
          f"price == 1 - yes_price — {s.no_mismatch} violations")
    if s.no_mismatch:
        fails.append(f"{label}: {s.no_mismatch} NO rows where price != 1-yes_price")

    print(f"{OK if not s.nonpos else BAD} {label}: quantity > 0 — "
          f"{s.nonpos} violations")
    if s.nonpos:
        fails.append(f"{label}: {s.nonpos} non-positive quantities")
    return fails


def gamma_volume_shares(condition_id: str) -> float | None:
    """Gamma's own share-volume counter for a market, or None if unavailable.

    Independent of the trades we paged — Gamma computes it from its own indexer —
    so it is the check that catches a truncated fetch. `volumeNum` is in SHARES,
    which is why it is compared against sum(quantity) and not against notional.

    The filter param is `condition_ids` (PLURAL). Two Gamma traps:
      - the singular `condition_id` is silently IGNORED and returns a page of
        unrelated markets, so we read `conditionId` back off every row;
      - `slug`/`condition_ids` search only OPEN markets unless `closed=true` is
        passed, so a settled market otherwise looks nonexistent.
    """
    url = (f"https://gamma-api.polymarket.com/markets"
           f"?condition_ids={condition_id}&closed=true")
    req = urllib.request.Request(url, headers={"User-Agent": "arb-research/0.1",
                                               "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"{WARN} polymarket: Gamma volume lookup failed ({e})")
        return None
    for m in data or []:
        if m.get("conditionId") == condition_id:   # verify, never trust
            v = m.get("volumeNum") or m.get("volume")
            return float(v) if v is not None else None
    return None


def dataapi_newest(condition_id: str) -> dict | None:
    """The single newest trade for a market, straight from the API."""
    url = (f"{POLY_TRADES}?condition_id={condition_id}&limit=1&taker_only=true")
    req = urllib.request.Request(url, headers={"User-Agent": "arb-research/0.1",
                                               "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            rows = json.loads(resp.read().decode("utf-8")).get("data") or []
    except Exception as e:
        print(f"{WARN} polymarket: head lookup failed ({e})")
        return None
    return rows[0] if rows else None


def validate_polymarket(ev) -> list[str]:
    fails = []
    clean_path = ev.poly_clean_csv()
    if clean_path is None:
        print(f"{WARN} polymarket: no clean tape, skipping")
        return fails
    source = {ev.poly_rpc_clean_csv: "rpc",
              ev.poly_dataapi_clean_csv: "dataapi",
              ev.poly_subgraph_clean_csv: "subgraph"}.get(clean_path, "?")

    print(f"\n--- POLYMARKET ({source}) ---")
    s = scan(clean_path)
    fails += check_structure(s, "polymarket")

    shares, vol = s.shares, s.notional

    # COMPLETENESS 1 — volume against Gamma's independent counter.
    # This is the replacement for the subgraph's `orderbook.tradesQuantity`
    # check: that endpoint was deprecated in Polymarket's V2 migration.
    # Compared on SHARES, which is both what Gamma reports and the quantity the
    # arbitrage actually consumes.
    gv = gamma_volume_shares(ev.poly_condition_id)
    if gv is None:
        print(f"{WARN} polymarket: no Gamma volume to compare against")
    else:
        ratio = shares / gv if gv else float("inf")
        # 1% tolerance: our tape is trade-level and Gamma's counter is its own
        # rollup, so they agree closely but not bit-for-bit.
        ok = abs(ratio - 1.0) <= 0.01
        mark = OK if ok else (BAD if ratio < 1.0 else WARN)
        print(f"{mark} polymarket: shares {shares:,.0f} vs Gamma "
              f"volumeNum {gv:,.0f}  (ratio {ratio:.4f})")
        if not ok:
            # THE DIRECTION OF THE GAP DECIDES WHETHER THIS IS A FAILURE.
            # This check exists to catch a TRUNCATED fetch, and a short fetch
            # can only LOSE volume — it can never invent any.
            #
            #   shares < Gamma  -> trades are missing. FAIL: that is exactly
            #                      the failure mode this check is here for.
            #
            #   shares > Gamma  -> Gamma's rollup is the low one. WARN only,
            #                      on direct evidence: presidential-2024 was
            #                      collected twice from fully independent
            #                      sources — the Goldsky subgraph (maker-fill
            #                      view, 2,749,706 rows) and Polymarket's own
            #                      Data API v2 (taker view, 2,359,913 rows) —
            #                      and the two agree on 1,568,716,366.5 shares
            #                      to 0.01 of a share, while BOTH sit 2.43%
            #                      above Gamma's 1,531,479,285. Two independent
            #                      pipelines do not converge to ten significant
            #                      figures on the same wrong number. Gamma
            #                      under-reports that market, so failing the
            #                      tape over it would be rejecting correct data
            #                      on a worse oracle's word.
            if ratio < 1.0:
                why = (f"tape is MISSING {(1 - ratio) * 100:.2f}% of Gamma's "
                       f"volume — fetch likely incomplete/truncated")
                print(f"         -> {why}")
                fails.append(f"polymarket: shares {shares:,.0f} vs Gamma "
                             f"{gv:,.0f} (ratio {ratio:.4f}) — {why}")
            else:
                print(f"         -> tape EXCEEDS Gamma by "
                      f"{(ratio - 1) * 100:.2f}%. NOT truncation (a short "
                      f"fetch cannot add volume), so this is a warning, not a "
                      f"failure.")
                print(f"            Gamma's counter is known to under-report "
                      f"some markets — see notes/polymarket_v2_migration.md. "
                      f"Confirm by re-collecting from a second source: if two "
                      f"independent tapes agree, trust them over Gamma.")

    # COMPLETENESS 2 — did we page to the HEAD of the tape?
    # This specifically guards the Data API's worst trap: its cursor does not
    # carry the market filter, so a paged request that forgets to re-send
    # `condition_id` returns 0 rows and `has_more: false`, which is
    # indistinguishable from end-of-data. The result is a tape of exactly one
    # page that reports success. Comparing our newest trade against the API's
    # newest catches that immediately.
    if source == "dataapi":
        newest = dataapi_newest(ev.poly_condition_id)
        if newest is not None:
            ours_ts = s.last_ts
            api_ts = int(newest["timestamp"])
            ok = ours_ts == api_ts
            print(f"{OK if ok else BAD} polymarket: newest trade matches API "
                  f"head ({api_ts})" + ("" if ok else f" — ours {ours_ts}"))
            if not ok:
                fails.append(f"polymarket: newest clean trade {ours_ts} != API "
                             f"head {api_ts} (tape truncated)")
            # A one-page tape is the signature of the cursor-filter bug.
            if s.n <= 1000:
                print(f"{WARN} polymarket: only {s.n} rows — if this is "
                      f"exactly one page, check that the fetch re-sends "
                      f"condition_id on every page")

    print(f"         clean trades: {s.n:,}   shares: {shares:,.0f}   "
          f"USDC volume: ${vol:,.0f}")
    print(f"         date range  : {s.first_iso[:10]} .. {s.last_iso[:10]}")
    print(f"         by outcome  : {dict(s.by_outcome)}")
    return fails


def validate_kalshi(ev) -> list[str]:
    fails = []
    clean_path = ev.kalshi_clean_csv
    if not clean_path.exists():
        print(f"{WARN} kalshi: no clean tape, skipping")
        return fails

    print("\n--- KALSHI ---")
    s = scan(clean_path)
    fails += check_structure(s, "kalshi")

    # Completeness: the API returns trades newest-first, so page 1 holds the most
    # recent trade. If our tape's newest trade is that trade, we paged to the end.
    url = f"{KALSHI_TRADES}?ticker={ev.kalshi_ticker}&limit=1"
    req = urllib.request.Request(url, headers={"User-Agent": "arb-research/0.1"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        newest = json.loads(resp.read().decode("utf-8"))["trades"][0]
    ours = s.last_id
    ok = ours == newest["trade_id"]
    print(f"{OK if ok else BAD} kalshi: newest trade matches API head "
          f"({newest['trade_id'][:8]}…)")
    if not ok:
        fails.append(f"kalshi: newest clean trade {ours[:8]}… != API head "
                     f"{newest['trade_id'][:8]}… (tape may be truncated)")

    # side must be BUY everywhere — Kalshi exposes no buy/sell field (see
    # clean_kalshi_trades.py docstring). Anything else means the old bug is back.
    sides = s.sides
    ok = sides == {"BUY"}
    print(f"{OK if ok else BAD} kalshi: side is always BUY — found {sides}")
    if not ok:
        fails.append(f"kalshi: unexpected side values {sides}")

    print(f"         clean trades: {s.n:,}   contracts: {s.shares:,.0f}   "
          f"notional: ${s.notional:,.0f}")
    print(f"         date range  : {s.first_iso[:10]} .. {s.last_iso[:10]}")
    print(f"         by outcome  : {dict(s.by_outcome)}")
    return fails


def report_overlap(ev) -> None:
    """The only window where the two venues are actually comparable."""
    poly_path = ev.poly_clean_csv()
    if not (ev.kalshi_clean_csv.exists() and poly_path):
        return
    from datetime import datetime, timezone
    from statistics import median
    fmt = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")

    # Two streaming passes per tape instead of holding both in memory: the
    # bounds first, then the daily sums. Nothing here needs the rows kept.
    def bounds(path):
        first = last = None
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                t = int(r["unix_ts"])
                if first is None:
                    first = t
                last = t
        return first, last

    k_lo, k_hi = bounds(ev.kalshi_clean_csv)
    p_lo, p_hi = bounds(poly_path)
    if None in (k_lo, p_lo):
        return
    lo, hi = max(k_lo, p_lo), min(k_hi, p_hi)
    if lo >= hi:
        print("\n--- OVERLAP ---\n  none (the venues never traded concurrently)")
        return

    # Compare DAILY means, not a pooled mean over trades. A pooled mean is
    # composition-weighted: whichever venue happens to trade more heavily on
    # high-price days looks systematically different even when the two price
    # paths are on top of each other. (On this event a pooled mean showed a
    # 0.13 "gap" where the real daily gap is ~0.03.)
    def by_day(path):
        """Daily mean yes_price inside [lo, hi], plus the trade count."""
        tot: dict[str, float] = {}
        cnt: dict[str, int] = {}
        n = 0
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                t = int(r["unix_ts"])
                if not lo <= t <= hi:
                    continue
                day = r["timestamp"][:10]
                tot[day] = tot.get(day, 0.0) + float(r["yes_price"])
                cnt[day] = cnt.get(day, 0) + 1
                n += 1
        return {d: tot[d] / cnt[d] for d in tot}, n

    kd, n_k = by_day(ev.kalshi_clean_csv)
    pd_, n_p = by_day(poly_path)
    days = sorted(set(kd) & set(pd_))
    gaps = [kd[d] - pd_[d] for d in days]

    print("\n--- OVERLAP (both venues trading) ---")
    print(f"         window        : {fmt(lo)} .. {fmt(hi)}  ({len(days)} days)")
    print(f"         kalshi        : {n_k:,} trades")
    print(f"         polymarket    : {n_p:,} trades")
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
