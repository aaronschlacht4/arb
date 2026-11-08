"""
Collect the COMPLETE historical trade history for one Kalshi market and save it
to a raw JSONL file, exactly as received from the API (project Rule 3).

Source endpoint (the "historical" tier — settled / older-than-~65-day markets):
    GET https://api.elections.kalshi.com/trade-api/v2/historical/trades

Key facts we verified empirically before writing this:
  - The filter parameter is `ticker` (NOT `market_ticker`, which is silently
    ignored and returns unrelated trades).
  - Pagination is cursor-based: echo back the returned `cursor`; stop when it is "".
  - Trades come newest -> oldest. Page size max is 1000.
  - No authentication is required to read trades.

Run (event-driven — ticker + output path come from events/<slug>/event.json):
    python src/fetch_kalshi_trades.py mamdani-dem-nomination
"""

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

from eventlib import (load_event, read_checkpoint, salvage_tape,
                      tail_json_lines, truncate_tape, write_checkpoint)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
HISTORICAL_TRADES_URL = f"{BASE_URL}/historical/trades"


def fetch_page(ticker: str, cursor: str | None, limit: int = 1000) -> dict:
    """
    Fetch a single page of historical trades.

    Returns the parsed JSON: {"cursor": "<next>", "trades": [ {...}, ... ]}.
    Retries a few times on transient errors (e.g. HTTP 429 rate limiting).
    """
    params = {"ticker": ticker, "limit": limit}
    if cursor:
        params["cursor"] = cursor
    url = f"{HISTORICAL_TRADES_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "arb-research/0.1"})

    last_err = None
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            # 429 = rate limited, 5xx = transient server error: back off and retry.
            if e.code in (429, 500, 502, 503, 504):
                last_err = e
                time.sleep(1.5 * (attempt + 1))
                continue
            raise  # 4xx other than 429 is a real bug (bad params) — surface it.
        except (OSError, json.JSONDecodeError) as e:
            # read timeout / connection reset / truncated body — transient, retry.
            last_err = e
            time.sleep(1.5 * (attempt + 1))
            continue
    raise RuntimeError(f"Gave up after retries on {url}: {last_err}")


CKPT_VERSION = 1


def collect(ticker: str, out_path: Path, limit: int = 1000,
            sleep_s: float = 0.2) -> Path:
    """
    Page through ALL historical trades for `ticker` and write them to
    `out_path`, one raw trade object per line.

    Validation built in:
      - ticker match: every row's `ticker` must equal the requested ticker,
        else we stop immediately (guards against the `market_ticker` trap).
      - dedup: skip any repeated `trade_id` (cursor paging should not repeat,
        but we verify rather than assume).

    RESUMABLE. The cursor and the running count are checkpointed after every
    page, so a run killed partway continues instead of starting over. This is
    not a nicety on the big markets: PRES-2024-DJT is 287k trades over ~290
    pages, and losing that to a dropped connection means re-paging the lot.
    The checkpoint is deleted on clean completion, so a finished tape is
    protected by the Rule-3 overwrite guard again.
    """
    ckpt_path = out_path.with_suffix(".checkpoint.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ck = None
    prev = read_checkpoint(ckpt_path)
    if prev is not None:
        if prev.get("version") == CKPT_VERSION and prev.get("ticker") == ticker:
            ck = prev
            print(f"Resuming from checkpoint ({ck['written']:,} trades written)",
                  file=sys.stderr)
        else:
            print("Checkpoint is for a different market/version; ignoring.",
                  file=sys.stderr)

    if ck is None:
        if out_path.exists():
            # Rule 3: never silently overwrite raw data.
            raise SystemExit(
                f"Refusing to overwrite existing raw file: {out_path}\n"
                "Move or delete it deliberately if you intend to re-acquire.")
        ck = {"version": CKPT_VERSION, "ticker": ticker, "written": 0,
              "cursor": None, "boundary_ids": []}

    cursor: str | None = ck.get("cursor")
    # Only the PREVIOUS page's ids can recur under cursor paging, so remembering
    # just those keeps the duplicate guard exact at flat memory — the full set
    # would not survive a resume anyway.
    seen_ids: set[str] = set(ck.get("boundary_ids", []))

    # --- make the tape safe to append to before writing a single byte -------
    if ck["written"]:
        # Each page is written as one COMPLETE unit and the resulting file size
        # is checkpointed, so cutting back to that offset discards a
        # half-written page. Without it, a kill mid-write leaves a truncated
        # final line that the next append FUSES to the next record — destroying
        # both and leaving a line no cleaner can parse.
        if ck.get("bytes_on_disk") is not None:
            truncate_tape(out_path, False, ck["bytes_on_disk"])
        else:
            print("  checkpoint has no byte offset — salvaging the tape",
                  file=sys.stderr)
            kept, _ = salvage_tape(out_path, False)
            print(f"  salvaged {kept:,} intact records", file=sys.stderr)

        # `limit` can differ between runs, so prime over the WIDER of the two
        # page sizes — a window sized to a smaller new limit would not cover a
        # larger replayed page.
        prime = 2 * max(limit, int(ck.get("limit") or limit))
        tail = tail_json_lines(out_path, prime)
        seen_ids |= {t.get("trade_id") for t in tail}
        print(f"  (dedupe primed with {len(tail):,} trades from the tape's tail)",
              file=sys.stderr)

    n_written = ck["written"]
    n_pages = 0

    while True:
        data = fetch_page(ticker, cursor, limit)
        trades = data.get("trades", [])
        if not trades:
            break

        page_ids = set()
        out_lines = []
        for t in trades:
            got = t.get("ticker")
            if got != ticker:
                raise SystemExit(
                    f"Ticker contamination: received {got!r} != requested {ticker!r}. "
                    "Aborting so we never save the wrong market's data."
                )
            tid = t.get("trade_id")
            page_ids.add(tid)
            if tid in seen_ids:
                continue  # duplicate guard
            # Write the trade object verbatim — no fields added, removed, or renamed.
            out_lines.append(json.dumps(t) + "\n")

        # ONE page = ONE complete unit: the handle is closed before the
        # checkpoint records the file size, so that size is always a safe
        # resume point and the tape never ends mid-record between pages.
        with out_path.open("a", encoding="utf-8") as f:
            f.writelines(out_lines)
        n_written += len(out_lines)

        n_pages += 1
        seen_ids = page_ids
        cursor = data.get("cursor", "")

        ck.update(written=n_written, cursor=cursor, limit=limit,
                  bytes_on_disk=out_path.stat().st_size,
                  boundary_ids=sorted(page_ids))
        write_checkpoint(ckpt_path, ck)

        print(f"  page {n_pages:>4}: +{len(trades):>4}  (total {n_written:,})",
              file=sys.stderr)
        if not cursor:
            break
        time.sleep(sleep_s)  # be polite to the API

    # Clean completion: drop the checkpoint so the finished tape is covered by
    # the overwrite guard above.
    ckpt_path.unlink(missing_ok=True)
    print(f"\nDone: {n_written:,} trades across {n_pages} pages -> {out_path}",
          file=sys.stderr)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Download complete Kalshi historical trades for an event's market.")
    parser.add_argument("event", help="Event slug, e.g. mamdani-dem-nomination")
    parser.add_argument("--limit", type=int, default=1000, help="Page size (max 1000)")
    args = parser.parse_args()
    ev = load_event(args.event)
    print(f"Fetching Kalshi trades for {ev.kalshi_ticker} -> {ev.kalshi_raw_jsonl}",
          file=sys.stderr)
    collect(ev.kalshi_ticker, ev.kalshi_raw_jsonl, limit=args.limit)


if __name__ == "__main__":
    main()
