"""
Collect RAW Polymarket trade primitives for an event's market via the GraphQL
SUBGRAPH (Wendy's prescribed method), instead of scanning eth_getLogs.

Queries the Polymarket orderbook subgraph's `enrichedOrderFilled` entity (which
has an INDEXED `market` field = the outcome-token id, plus pre-decoded
price/size/side/timestamp), pages through every fill for each of the market's
two token ids, and saves each record VERBATIM to JSONL.

WHY THIS IS FAST: unlike the raw `eth_getLogs` scan (which must read every block
because OrderFilled doesn't index the token id), the subgraph indexes `market`,
so we only fetch this market's fills — minutes, not hours.

PAGINATION — WHY WE ORDER BY `timestamp`, NOT `id` (measured 2026-07-12 against
the Trump popular-vote market, ~366k fills):
  - `orderBy: id` (a STRING column) trips the subgraph's Postgres statement
    timeout above ~50 rows/page. 366k fills / 50 = 7000+ pages ≈ 75 minutes.
  - `orderBy: timestamp` (an INDEXED numeric column) serves 750 rows in ~1.5s.
    1000 still times out, so PAGE defaults to 500 for margin.

  The catch: timestamps COLLIDE (many fills share one second), so a timestamp
  cursor must NOT simply advance to `last_ts + 1` — that would silently DROP the
  rest of the fills in that second. Instead we:
    1. advance the cursor to `last_ts` (not +1) and refetch, so ties are never
       skipped;
    2. dedupe the resulting overlap by `id`, remembering only the ids seen AT
       THE BOUNDARY SECOND (not all 366k — keeps memory flat);
    3. if an entire page turns out to be a SINGLE second (>PAGE fills in one
       second — possible on election night), fall back to draining that one
       second by `id_gt`, which is cheap because it is scoped to one timestamp,
       then step to `ts + 1`.
  This is lossless: no fill is skipped and none is written twice.

NOTE (verified empirically): `enrichedOrderFilled` is NOT deduped — it includes
both the per-maker fills AND the aggregate taker-order rows (taker == Exchange).
The aggregate rows are dropped in the SEPARATE cleaning step.

ENDPOINT: defaults to the public Goldsky endpoint documented in
notes/polymarket_validation.md. Override with POLYMARKET_SUBGRAPH_URL in .env.

Token ids + output path come from events/<slug>/event.json.

Run:
    python src/fetch_polymarket_subgraph.py popvote-2024
    SUBGRAPH_PAGE=750 python src/fetch_polymarket_subgraph.py popvote-2024
"""

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

from eventlib import load_event, open_raw, raw_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

# Public, no-auth subgraph (Goldsky). Overridable via .env for a private host.
DEFAULT_SUBGRAPH_URL = (
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw"
    "/subgraphs/polymarket-orderbook-resync/prod/gn"
)

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}

# 750 is the measured ceiling before the statement timeout; 500 leaves margin.
PAGE = int(os.environ.get("SUBGRAPH_PAGE", "500"))

CKPT_VERSION = 2  # v1 paged by id; v2 pages by timestamp (cursor is incompatible)

# Bounded-window fallback for the sparse tail (see fetch_token). 1 day is safe
# even through election week; quiet stretches widen automatically up to 30 days.
WINDOW_START = 86_400
WINDOW_MIN = 300
WINDOW_MAX = 30 * 86_400

# Primary page: the next PAGE fills at or after `ts`, oldest first.
QUERY_BY_TS = """
query($market: String!, $ts: Int!, $first: Int!) {
  enrichedOrderFilleds(
    first: $first
    orderBy: timestamp
    orderDirection: asc
    where: { market: $market, timestamp_gte: $ts }
  ) {
    id
    transactionHash
    timestamp
    price
    size
    side
    maker { id }
    taker { id }
  }
}
"""

# Tail page: same as QUERY_BY_TS but BOUNDED ABOVE by `tsEnd`. See the
# bounded-window note in fetch_token() for why this is needed.
QUERY_BY_TS_WINDOW = """
query($market: String!, $ts: Int!, $tsEnd: Int!, $first: Int!) {
  enrichedOrderFilleds(
    first: $first
    orderBy: timestamp
    orderDirection: asc
    where: { market: $market, timestamp_gte: $ts, timestamp_lt: $tsEnd }
  ) {
    id
    transactionHash
    timestamp
    price
    size
    side
    maker { id }
    taker { id }
  }
}
"""

# Fallback page: drain ONE second by id. Scoped to a single timestamp, so the
# otherwise-slow `orderBy: id` is cheap here.
QUERY_BY_ID_IN_SECOND = """
query($market: String!, $ts: Int!, $lastId: String!, $first: Int!) {
  enrichedOrderFilleds(
    first: $first
    orderBy: id
    orderDirection: asc
    where: { market: $market, timestamp: $ts, id_gt: $lastId }
  ) {
    id
    transactionHash
    timestamp
    price
    size
    side
    maker { id }
    taker { id }
  }
}
"""


def load_endpoint() -> str:
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            if line.startswith("POLYMARKET_SUBGRAPH_URL="):
                url = line.split("=", 1)[1].strip()
                if url:
                    return url
    return DEFAULT_SUBGRAPH_URL


class SubgraphTimeout(RuntimeError):
    """The subgraph's Postgres statement timeout killed the query.

    Distinct from a generic error because the CALLER can fix it — by shrinking
    the page or bounding the time window — whereas a real error must surface.
    """


def gql(url: str, query: str, variables: dict, tries: int = 8) -> list:
    """One page of enrichedOrderFilled, with retry/backoff on transient errors.

    A statement timeout is retried a few times (the subgraph may just be under
    load), then raised as SubgraphTimeout so the caller can re-shape the query.
    We never return partial data silently.
    """
    body = json.dumps({"query": query, "variables": variables}).encode()
    last_err = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, data=body, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=90) as resp:
                out = json.loads(resp.read().decode("utf-8"))
            if "errors" in out:
                msg = str(out["errors"]).lower()
                if "timeout" in msg or "timed out" in msg:
                    last_err = SubgraphTimeout(msg)
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise RuntimeError(out["errors"])
            return out["data"]["enrichedOrderFilleds"]
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                last_err = e
                time.sleep(1.5 * (attempt + 1))
                continue
            raise
        except (OSError, json.JSONDecodeError) as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))
            continue
    # A SOCKET read-timeout means the same thing as a Postgres statement timeout:
    # the query we asked for is too heavy to serve. Classify it as SubgraphTimeout
    # so the caller shrinks the window instead of dying — otherwise it escapes as
    # a bare RuntimeError and kills the shard (this bit shard 18 of presidential-2024).
    msg = str(last_err).lower()
    if isinstance(last_err, (SubgraphTimeout, TimeoutError)) or "timed out" in msg \
            or "timeout" in msg:
        raise SubgraphTimeout(f"still timing out after {tries} tries: {last_err}")
    raise RuntimeError(f"subgraph page failed after {tries} tries: {last_err}")


def gql_adaptive(url: str, query: str, base_vars: dict, page: int,
                 floor: int = 25) -> tuple[list, int]:
    """Fetch one page, HALVING the page size on persistent statement timeouts.

    The subgraph's Postgres timeout is load-dependent, not a fixed row ceiling:
    a page size that worked for months of history can start timing out in a
    busier stretch of the tape (election week). Retrying the same oversized
    query just fails again, so we shrink it — same idea as the adaptive block
    span in fetch_polymarket_rpc.py.

    Returns (rows, page_size_actually_used). The caller MUST use the returned
    size for its short-page "we're done" test — comparing against the global
    PAGE would misread a reduced page as end-of-data and truncate the tape.
    """
    p = page
    while True:
        try:
            return gql(url, query, {**base_vars, "first": p}, tries=4), p
        except SubgraphTimeout:
            if p <= floor:
                raise  # page size is not the problem — caller must re-shape
            p = max(floor, p // 2)
            print(f"    statement timeout -> reducing page size to {p}",
                  flush=True)


def write_rows(f, rows: list, market: str, skip_ids: set) -> int:
    """Append the rows we have not already written. Returns the count written."""
    n = 0
    for r in rows:
        if r["id"] in skip_ids:
            continue
        # This subgraph omits `market` in responses, so stamp each record with
        # the token id we queried it under (the outcome-token id the cleaner
        # maps to YES/NO). This is the ONLY field we add to the raw record.
        r.setdefault("market", market)
        f.write(json.dumps(r) + "\n")
        skip_ids.add(r["id"])
        n += 1
    return n


def drain_second(f, url: str, market: str, ts: int, written_at_ts: set) -> int:
    """>PAGE fills share one second: page that single second by id until dry."""
    n = 0
    last_id = ""
    while True:
        rows, used = gql_adaptive(
            url, QUERY_BY_ID_IN_SECOND,
            {"market": market, "ts": ts, "lastId": last_id}, PAGE)
        if not rows:
            break
        n += write_rows(f, rows, market, written_at_ts)
        last_id = rows[-1]["id"]
        if len(rows) < used:  # `used`, not PAGE — see gql_adaptive docstring
            break
    return n


def fetch_token(f, url: str, name: str, market: str, ck: dict, ckpt_path: Path,
                t0: float, start_ts: int = 0, stop_ts: int | None = None,
                force_bounded: bool = False) -> None:
    """Page every fill for one outcome token in [start_ts, stop_ts), losslessly.

    SHARDING: pass start_ts/stop_ts to restrict this worker to a slice of the
    tape. Ranges are HALF-OPEN — a fill at exactly stop_ts belongs to the NEXT
    shard, so 8 workers over contiguous ranges see every fill exactly once, with
    no boundary double-count. Sharded runs force bounded-window mode, since a
    shard has an upper bound by definition.

    TWO MODES, because one query shape cannot cover the whole tape:

    1. UNBOUNDED (fast, default) — `timestamp_gte: cursor`, no upper bound.
       While this market's fills are DENSE, the planner fills the LIMIT almost
       immediately. This carries ~99.9% of the tape at ~500 rows/1.2s.

    2. BOUNDED WINDOW (slow, fallback) — `timestamp_gte: cursor,
       timestamp_lt: cursor + window`.
       Near the END of the tape the market is nearly resolved and our fills go
       SPARSE. The planner then flips to a timestamp-index scan expecting a quick
       hit and instead wades through millions of OTHER markets' rows looking for
       ours, until the statement timeout kills it. Shrinking the page makes this
       strictly WORSE (fewer rows to find, same scan). Measured at the 2024-11-11
       stall: unbounded `first:1` times out; bounded to +1 day returns instantly;
       bounded to +30 days times out again — the cost scales with TOTAL platform
       activity in the window, not with our market's. So we bound the window and
       adapt its width.

    We start unbounded and switch to bounded on the first unrecoverable timeout,
    walking windows to STOP_TS (now). The window grows when empty and shrinks on
    timeout, so it self-tunes to whatever the indexer can serve.
    """
    state = ck["tokens"][name]
    if state.get("done"):
        print(f"  {name}: already complete, skipping")
        return

    # A fresh shard starts at its range start; a resumed one at its checkpoint.
    cursor_ts = max(int(state["cursor_ts"]), start_ts)
    # ids already written AT cursor_ts — the only possible duplicates, because we
    # refetch from timestamp_gte=cursor_ts rather than cursor_ts+1.
    boundary_ids = set(state.get("boundary_ids", []))

    stop_ts = int(time.time()) if stop_ts is None else stop_ts
    page = PAGE          # shrinks on timeout, recovers below
    window = WINDOW_START
    bounded = force_bounded
    ok_streak = 0

    # A bounded walk starting at 0 would step through DECADES of empty windows
    # before reaching the token's first fill (the subgraph indexes all of
    # Polymarket, back to 2020). Jump straight to the earliest fill instead.
    # This one query is the DENSE case (asc from 0), so it is fast.
    if bounded and cursor_ts <= 0:
        try:
            first = gql(url, QUERY_BY_TS,
                        {"market": market, "ts": 0, "first": 1}, tries=3)
        except SubgraphTimeout:
            first = None  # fall back to the (correct, just slow) empty walk
        if first == []:
            print(f"  {name}: no fills at all for this token")
            state["done"] = True
            ckpt_path.write_text(json.dumps(ck))
            return
        if first:
            cursor_ts = int(first[0]["timestamp"])
            print(f"  {name}: first fill at "
                  f"{time.strftime('%Y-%m-%d', time.gmtime(cursor_ts))} "
                  f"— starting there", flush=True)

    def log(last_ts: int, n_new: int) -> None:
        rate = ck["written"] / max(1e-9, time.time() - t0)
        day = time.strftime("%Y-%m-%d", time.gmtime(last_ts))
        tag = f"w={window // 3600}h" if bounded else f"p={page}"
        print(f"  {name}: +{n_new:<4} total={ck['written']:>8,}  "
              f"({rate:>5.0f}/s)  at {day}  [{tag}]", flush=True)

    def save(last_ts: int, n_new: int) -> None:
        state["cursor_ts"] = cursor_ts
        state["boundary_ids"] = sorted(boundary_ids)
        f.flush()
        ckpt_path.write_text(json.dumps(ck))
        log(last_ts, n_new)

    while True:
        # ---- fetch one page, in whichever mode we're in ----
        if not bounded:
            try:
                rows, used = gql_adaptive(url, QUERY_BY_TS,
                                          {"market": market, "ts": cursor_ts},
                                          page)
            except SubgraphTimeout:
                # Page size isn't the problem — the tail has gone sparse.
                print(f"    {name}: sparse tail — switching to bounded-window "
                      f"mode at {time.strftime('%Y-%m-%d', time.gmtime(cursor_ts))}",
                      flush=True)
                bounded = True
                page = PAGE
                continue
            page = used
            win_end = None
        else:
            if cursor_ts >= stop_ts:
                break  # walked the window to the present: tape exhausted
            win_end = min(cursor_ts + window, stop_ts)
            try:
                rows = gql(url, QUERY_BY_TS_WINDOW,
                           {"market": market, "ts": cursor_ts, "tsEnd": win_end,
                            "first": page}, tries=5)
            except SubgraphTimeout:
                if window <= WINDOW_MIN:
                    raise  # nothing left to shrink — fail loudly, don't truncate
                window = max(WINDOW_MIN, window // 4)
                print(f"    statement timeout -> shrinking window to "
                      f"{window}s", flush=True)
                continue
            used = page

        # ---- advance the cursor ----
        if not rows:
            if not bounded:
                break  # no more fills at all for this token
            # Empty window: skip it, and widen — quiet stretches are cheap.
            cursor_ts = win_end
            boundary_ids = set()
            window = min(WINDOW_MAX, window * 2)
            ok_streak = 0
            state["cursor_ts"] = cursor_ts
            state["boundary_ids"] = []
            ckpt_path.write_text(json.dumps(ck))
            continue

        ok_streak += 1
        if ok_streak >= 20 and page < PAGE:
            page = min(PAGE, page * 2)  # recover after a rough patch
            ok_streak = 0

        n_new = write_rows(f, rows, market, boundary_ids)
        ck["written"] += n_new

        first_ts = int(rows[0]["timestamp"])
        last_ts = int(rows[-1]["timestamp"])

        if len(rows) < used:
            # Short page. Unbounded: that's the end of the tape. Bounded: it just
            # means this WINDOW is exhausted — step to the next one, don't stop.
            if not bounded:
                save(last_ts, n_new)
                break
            cursor_ts = win_end
            boundary_ids = set()
            save(last_ts, n_new)
            continue

        if first_ts == last_ts:
            # The whole page is one second; a timestamp cursor cannot advance
            # without dropping fills. Drain that second by id, then step past it.
            ck["written"] += drain_second(f, url, market, last_ts, boundary_ids)
            cursor_ts = last_ts + 1
            boundary_ids = set()
        else:
            # Re-anchor on last_ts (NOT last_ts+1) so tied fills are never
            # skipped; remember only the ids already written at that second.
            boundary_ids = {r["id"] for r in rows
                            if int(r["timestamp"]) == last_ts}
            cursor_ts = last_ts

        save(last_ts, n_new)

    state["done"] = True
    state["cursor_ts"] = cursor_ts
    state["boundary_ids"] = sorted(boundary_ids)
    f.flush()
    ckpt_path.write_text(json.dumps(ck))
    print(f"  {name}: COMPLETE (total written so far {ck['written']:,})")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch raw Polymarket fills for an event's market via the "
                    "subgraph. Shard with --start-ts/--end-ts/--suffix, then "
                    "merge the parts with src/combine_subgraph_parts.py.")
    ap.add_argument("event", nargs="?", help="Event slug, e.g. popvote-2024")
    ap.add_argument("--start-ts", type=int, default=0,
                    help="shard range start, unix seconds (inclusive)")
    ap.add_argument("--end-ts", type=int, default=None,
                    help="shard range end, unix seconds (EXCLUSIVE)")
    ap.add_argument("--suffix", default="",
                    help="output-file suffix for a parallel shard, e.g. .part3")
    args = ap.parse_args()

    ev = load_event(args.event)
    if len(ev.poly_token_ids) < 2:
        raise SystemExit("event.json needs polymarket_token_ids: [YES, NO]")
    tokens = {"YES": ev.poly_token_ids[0], "NO": ev.poly_token_ids[1]}

    # Keep the PLAIN .jsonl name as the base: the checkpoint name is derived from
    # it, and `.with_suffix()` on a `.jsonl.gz` path would mangle the extension.
    base_path = ev.poly_subgraph_raw_jsonl
    if args.suffix:
        base_path = base_path.with_name(base_path.stem + args.suffix + ".jsonl")
    ckpt_path = base_path.with_suffix(".checkpoint.json")
    out_path = raw_path(base_path, ev.poly_raw_gzip)   # what actually gets written

    # A shard is bounded above, so it must walk bounded windows.
    sharded = bool(args.suffix) or args.end_ts is not None

    url = load_endpoint()
    print(f"event: {ev.slug}  condition: {ev.poly_condition_id}")
    print(f"endpoint: {url.split('/subgraphs/')[0]}/subgraphs/...  page={PAGE}")
    if sharded:
        fmt = lambda t: time.strftime("%Y-%m-%d", time.gmtime(t))
        end_show = fmt(args.end_ts) if args.end_ts else "now"
        print(f"SHARD {args.suffix or '(unnamed)'}: "
              f"[{fmt(args.start_ts)}, {end_show})  -> {out_path.name}")

    # Resume if a compatible checkpoint exists; otherwise start clean.
    ck = None
    if ckpt_path.exists():
        prev = json.loads(ckpt_path.read_text())
        if prev.get("version") == CKPT_VERSION:
            ck = prev
            print(f"Resuming from checkpoint ({ck['written']:,} records written)")
        else:
            print("Checkpoint predates timestamp-paging (v2); starting fresh.")
    if ck is None:
        if out_path.exists():
            # Rule 3: never silently clobber a raw acquisition.
            raise SystemExit(
                f"Raw file exists with no resumable checkpoint: {out_path}\n"
                "Move or delete it deliberately if you intend to re-acquire.")
        ck = {"version": CKPT_VERSION, "written": 0,
              "tokens": {n: {"cursor_ts": 0, "boundary_ids": [], "done": False}
                         for n in tokens}}

    t0 = time.time()
    with open_raw(base_path, "at", ev.poly_raw_gzip) as f:
        for name, market in tokens.items():
            fetch_token(f, url, name, market, ck, ckpt_path, t0,
                        start_ts=args.start_ts, stop_ts=args.end_ts,
                        force_bounded=sharded)

    print(f"\nDone. {ck['written']:,} enrichedOrderFilled records -> {out_path}")


if __name__ == "__main__":
    main()
