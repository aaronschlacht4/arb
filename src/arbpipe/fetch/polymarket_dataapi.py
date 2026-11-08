"""
Collect the COMPLETE trade history for one Polymarket market from the Polymarket
Data API v2, saving each record VERBATIM to JSONL (project Rule 3).

WHY THIS FILE EXISTS — the two older Polymarket paths are both DEAD as of
2026-11-04, verified live:

  1. Goldsky orderbook subgraph (`fetch_polymarket_subgraph.py`) returns:
        "This Polymarket subgraph endpoint is paused and deprecated following
         Polymarket's migration to V2 - the data is stale and incorrect."
     with `extensions.code = ENDPOINT_DEPRECATED`. It serves HTTP 429 and no
     data. Do not use it; even where it answers, the data is stale.
  2. Data API **v1** (`https://data-api.polymarket.com/trades`) returns
        HTTP 410  {"error": "Data API v1 has been retired", ...}

  Kalshi did NOT change: `/historical/trades?ticker=` still returns the same
  `count_fp` / `*_dollars` schema the cleaner already expects.

ENDPOINT
    GET https://data-api.polymarket.com/v2/trades?condition_id=<0x…>&limit=1000

Facts established empirically before writing this (all re-checkable):

  FILTER PARAM. `condition_id` (aliases `condition`, `conditionId`) is the
  correct filter. Two traps:
    - `market=<id>` is REJECTED with a helpful 400 — harmless.
    - `asset_id=<tokenId>` returns HTTP 200 and a page of a COMPLETELY
      UNRELATED market (we asked for the Mamdani token and got
      "Racing Club de Lens vs. Como 1907"). It is silently ignored, exactly
      like Kalshi's `market_ticker` trap. So we re-verify `condition_id` on
      EVERY row and abort on contamination rather than trusting the filter.

  NO TIME FILTERING. `start_ts`, `end_ts`, `from`, `to`, `min_timestamp` are all
  accepted with HTTP 200 and then IGNORED — a request bounded to before the
  market opened still returns the newest trades. There is therefore NO way to
  shard this source by time the way the subgraph fetcher did. The whole tape
  must be walked with the cursor, in one ordered pass.

  THROUGHPUT SWINGS ~10x AND YOU CANNOT PLAN AROUND THE FAST CASE. Same
  endpoint, same page size, a day apart: ~0.6 s/page (~1,600 rows/s) and
  ~7 s/page (~140-220 rows/s). The slow case still returns HTTP 200 with full
  1000-row pages — server-side throttling, not an error, so there is nothing to
  retry and no signal in the response. A 1M-row market is somewhere between 10
  minutes and 2 hours. This is exactly why the checkpoint below is not
  optional, and why running two fetches at once is counterproductive: they
  share one budget.

  PAGINATION. `limit` max is 1000 (1001+ is a 400). Response is
  `{"data": [...], "pagination": {limit, offset, has_more, next_cursor}}`.
  Echo `next_cursor` back as `cursor`; stop when `has_more` is false. Trades
  come NEWEST -> OLDEST and timestamps are monotonically non-increasing.
  `offset` is cosmetic — this API is cursor-only.

  *** THE CURSOR DOES NOT CARRY THE FILTER. *** The single worst trap here.
  The cursor decodes to just a page size and a `(block_timestamp, sequence_id)`
  anchor plus a signature — NO market filter. So a request with the cursor ALONE
  returns 0 rows and `has_more: false`, which is indistinguishable from
  end-of-data. A naive cursor-follow therefore collects exactly one page and
  then reports success with a tape that is 1000 trades long.
  We re-send `condition_id` AND `taker_only` on EVERY page for this reason.
  The completeness check in validate_event.py exists to catch it if this
  regresses.

  THREE-YEAR WINDOW (a real limit of this source). `start` / `end` are honored
  only on the `user` query shape; filtering by condition serves a fixed ~3-year
  window. Markets that stopped trading before roughly Nov 2023 return 0 rows
  and cannot be fetched this way at all — a 2021 market was verified empty.
  Every event currently in this repo is inside the window. For older markets the
  fallback is Goldsky's paid `/polymarket/activity` feed, keyed by token_id.

  `takerOnly` — WHICH VIEW WE TAKE, AND WHY.
    `takerOnly=true` is the DEFAULT and is what we save: ONE row per economic
    trade, from the TAKER's side.
    `takerOnly=false` additionally emits the COUNTERPARTY row for each trade —
    same tx, same size, same price, OPPOSITE side, the maker's wallet. It is an
    exact doubling, not extra information, and summing it double-counts every
    dollar of volume.
    This is a real simplification over the subgraph, which required dropping
    aggregate `taker == exchange` rows and flipping the maker-side `side`.
    Neither correction applies here: see clean_polymarket_dataapi.py.

  ROW IDENTITY. In the taker view `transaction_hash` was unique across every
  row we checked (12,000/12,000 distinct). We do NOT assume it: the dedupe key
  is the full economic tuple, and the cleaner derives a `trade_id` that stays
  unique even if one tx ever carries two taker orders.

Run (condition id + output path come from events/<slug>/event.json):
    python arb.py fetch-poly mamdani-nyc-mayor
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from arbpipe.eventlib import (load_event, open_raw, raw_path, salvage_tape,
                      read_checkpoint, tail_json_lines, tape_size,
                      truncate_tape, write_checkpoint)

TRADES_URL = "https://data-api.polymarket.com/v2/trades"
PAGE_MAX = 1000
HEADERS = {"User-Agent": "arb-research/0.1", "Accept": "application/json"}

CKPT_VERSION = 1


def fetch_page(condition_id: str, cursor: str | None, limit: int,
               tries: int = 6) -> dict:
    """One page of trades, retrying transient failures (429 / 5xx / socket).

    `condition_id` and `taker_only` are re-sent on EVERY page, cursor or not:
    the cursor carries neither (see the module docstring). Dropping them on
    paged requests silently truncates the tape to one page; dropping
    `taker_only` would silently double it if the API's default ever flips.
    """
    params = {"condition_id": condition_id, "limit": limit,
              "taker_only": "true"}
    if cursor:
        params["cursor"] = cursor
    url = f"{TRADES_URL}?{urllib.parse.urlencode(params)}"

    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=90) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                last = e
                time.sleep(1.5 * (attempt + 1))
                continue
            # 400/410 is a real problem (bad param, retired endpoint) — surface
            # the body, which on this API explains what to use instead.
            body = ""
            try:
                body = e.read().decode("utf-8")[:400]
            except Exception:
                pass
            raise RuntimeError(f"HTTP {e.code} on {url}\n  {body}") from e
        except (OSError, json.JSONDecodeError) as e:
            last = e
            time.sleep(1.5 * (attempt + 1))
            continue
    raise RuntimeError(f"Gave up after {tries} tries on {url}: {last}")


def row_key(r: dict) -> tuple:
    """Identity of one trade, for the duplicate guard.

    The full economic tuple rather than just the tx hash: one transaction can in
    principle carry more than one taker order, and two such orders are distinct
    trades that must both be kept.
    """
    return (r.get("transaction_hash"), r.get("token_id"), r.get("side"),
            r.get("price"), r.get("size"), r.get("proxy_wallet"),
            r.get("timestamp"))


def collect(condition_id: str, base_path: Path, use_gzip: bool,
            limit: int = PAGE_MAX, sleep_s: float = 0.05) -> int:
    """Page the whole tape newest -> oldest and append it to the raw file.

    Resumable: the opaque cursor plus the running count is checkpointed after
    every page, so an interrupted run continues instead of restarting. The
    cursor is signed and opaque, so we store it verbatim and never parse it.
    """
    out_path = raw_path(base_path, use_gzip)
    ckpt_path = base_path.with_suffix(".checkpoint.json")

    ck = None
    prev = read_checkpoint(ckpt_path)
    if prev is not None:
        if prev.get("version") == CKPT_VERSION and \
                prev.get("condition_id") == condition_id:
            ck = prev
            print(f"Resuming from checkpoint ({ck['written']:,} rows written)",
                  file=sys.stderr)
        else:
            print("Checkpoint is for a different market/version; ignoring.",
                  file=sys.stderr)
    if ck is None:
        if out_path.exists():
            # Rule 3: never silently clobber a raw acquisition.
            raise SystemExit(
                f"Raw file exists with no resumable checkpoint: {out_path}\n"
                "Move or delete it deliberately if you intend to re-acquire.")
        ck = {"version": CKPT_VERSION, "condition_id": condition_id,
              "written": 0, "cursor": None, "done": False,
              "seen_at_boundary": []}

    if ck.get("done"):
        print(f"Already complete: {ck['written']:,} rows in {out_path}",
              file=sys.stderr)
        return ck["written"]

    cursor = ck.get("cursor")
    # Only the PREVIOUS page's keys can recur, because the cursor advances
    # strictly. Keeping just those bounds memory at one page instead of 600k.
    prev_keys = {tuple(k) for k in ck.get("seen_at_boundary", [])}

    # --- make the tape safe to append to before writing a single byte -------
    if ck["written"]:
        # A page is written as one COMPLETE unit and its resulting file size is
        # checkpointed, so cutting back to that offset discards a half-written
        # page. Without this, appending to a killed gzip member makes the whole
        # tape undecodable past the kill point (see eventlib.salvage_tape).
        if ck.get("bytes_on_disk") is not None:
            truncate_tape(base_path, use_gzip, ck["bytes_on_disk"])
        else:
            # Checkpoint predates byte tracking: we do not know where the last
            # good page ended, so repair the tape by rewriting what decodes.
            print("  checkpoint has no byte offset — salvaging the tape",
                  file=sys.stderr)
            kept, _ = salvage_tape(base_path, use_gzip)
            print(f"  salvaged {kept:,} intact records", file=sys.stderr)

        # Re-prime the dedupe window from what survived. The cursor replays the
        # page it points at, and `2 * limit` always covers one replayed page.
        prime = 2 * max(limit, int(ck.get("limit") or limit))
        tail = tail_json_lines(base_path, prime, use_gzip)
        prev_keys |= {row_key(t) for t in tail}
        print(f"  (dedupe primed with {len(tail):,} rows from the tape's tail)",
              file=sys.stderr)

    n_written = ck["written"]
    n_pages = n_dupes = 0
    t0 = time.time()
    stopped_early = False

    while True:
        data = fetch_page(condition_id, cursor, limit)
        rows = data.get("data") or []
        pg = data.get("pagination") or {}
        has_more = bool(pg.get("has_more"))
        if not rows:
            # An empty page is ALSO what a dropped market filter looks like
            # (module docstring). Only treat it as end-of-tape when the API
            # agrees; otherwise stop WITHOUT marking the tape complete, so the
            # next run resumes instead of trusting a short tape forever.
            if has_more:
                print("  empty page while has_more=true — stopping WITHOUT "
                      "marking complete; re-run to resume", file=sys.stderr)
                stopped_early = True
            break

        page_keys = set()
        out_lines = []
        for r in rows:
            got = r.get("condition_id")
            if got != condition_id:
                # The `asset_id` trap (see module docstring): a bad filter
                # returns 200 with another market's trades. Abort rather
                # than save the wrong market's tape.
                raise SystemExit(
                    f"Condition contamination: received {got!r} != "
                    f"requested {condition_id!r}. Aborting so we never "
                    f"save the wrong market's data.")
            k = row_key(r)
            page_keys.add(k)
            if k in prev_keys:
                n_dupes += 1
                continue
            # Verbatim — no field added, removed, or renamed.
            out_lines.append(json.dumps(r) + "\n")

        # ONE page = ONE complete unit. Closing the handle finishes the gzip
        # member (trailer and all), so the file on disk is always decodable
        # between pages, and its size is a safe resume point.
        with open_raw(base_path, "at", use_gzip) as f:
            f.writelines(out_lines)
        n_written += len(out_lines)

        n_pages += 1
        prev_keys = page_keys
        cursor = pg.get("next_cursor")

        ck.update(written=n_written, cursor=cursor, limit=limit,
                  bytes_on_disk=tape_size(base_path, use_gzip),
                  seen_at_boundary=[list(k) for k in page_keys])
        write_checkpoint(ckpt_path, ck)

        rate = n_written / max(1e-9, time.time() - t0)
        day = time.strftime("%Y-%m-%d",
                            time.gmtime(int(rows[-1]["timestamp"])))
        print(f"  page {n_pages:>4}: +{len(rows):>4}  total {n_written:>8,}"
              f"  ({rate:>5.0f}/s)  back to {day}", file=sys.stderr)

        if not has_more or not cursor:
            break
        time.sleep(sleep_s)

    ck.update(written=n_written, done=not stopped_early)
    write_checkpoint(ckpt_path, ck)
    print(f"\nDone: {n_written:,} trades across {n_pages} pages "
          f"({n_dupes:,} duplicates skipped) -> {out_path}", file=sys.stderr)
    return n_written


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Download the complete Polymarket trade history for an "
                    "event's market via the Data API v2.")
    ap.add_argument("event", nargs="?", help="Event slug, e.g. mamdani-nyc-mayor")
    ap.add_argument("--limit", type=int, default=PAGE_MAX,
                    help=f"page size (max {PAGE_MAX})")
    args = ap.parse_args()

    if not 0 < args.limit <= PAGE_MAX:
        raise SystemExit(f"--limit must be in 1..{PAGE_MAX}")

    ev = load_event(args.event)
    base = ev.poly_dataapi_raw_jsonl
    print(f"event: {ev.slug}", file=sys.stderr)
    print(f"condition: {ev.poly_condition_id}", file=sys.stderr)
    print(f"-> {raw_path(base, ev.poly_raw_gzip)}", file=sys.stderr)
    collect(ev.poly_condition_id, base, ev.poly_raw_gzip, limit=args.limit)


if __name__ == "__main__":
    main()
