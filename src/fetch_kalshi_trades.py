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

Run:
    python src/fetch_kalshi_trades.py KXMAYORNYCPARTY-25-D
"""

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
HISTORICAL_TRADES_URL = f"{BASE_URL}/historical/trades"

# Where raw files live. Resolved relative to the project root (this file's parent's parent).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"


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
    raise RuntimeError(f"Gave up after retries on {url}: {last_err}")


def collect(ticker: str, limit: int = 1000, sleep_s: float = 0.2) -> Path:
    """
    Page through ALL historical trades for `ticker` and write them to
    data/raw/kalshi_trades_<ticker>.jsonl, one raw trade object per line.

    Validation built in:
      - ticker match: every row's `ticker` must equal the requested ticker,
        else we stop immediately (guards against the `market_ticker` trap).
      - dedup: skip any repeated `trade_id` (cursor paging should not repeat,
        but we verify rather than assume).
    """
    out_path = RAW_DIR / f"kalshi_trades_{ticker}.jsonl"
    if out_path.exists():
        # Rule 3: never silently overwrite raw data.
        raise SystemExit(f"Refusing to overwrite existing raw file: {out_path}")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    seen_ids: set[str] = set()
    n_written = 0
    n_pages = 0
    cursor: str | None = None

    with out_path.open("w", encoding="utf-8") as f:
        while True:
            data = fetch_page(ticker, cursor, limit)
            trades = data.get("trades", [])
            if not trades:
                break

            for t in trades:
                got = t.get("ticker")
                if got != ticker:
                    raise SystemExit(
                        f"Ticker contamination: received {got!r} != requested {ticker!r}. "
                        "Aborting so we never save the wrong market's data."
                    )
                tid = t.get("trade_id")
                if tid in seen_ids:
                    continue  # duplicate guard
                seen_ids.add(tid)
                # Write the trade object verbatim — no fields added, removed, or renamed.
                f.write(json.dumps(t) + "\n")
                n_written += 1

            n_pages += 1
            cursor = data.get("cursor", "")
            print(f"  page {n_pages:>3}: +{len(trades):>4}  (total {n_written})", file=sys.stderr)
            if not cursor:
                break
            time.sleep(sleep_s)  # be polite to the API

    print(f"\nDone: {n_written} trades across {n_pages} pages -> {out_path}", file=sys.stderr)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Download complete Kalshi historical trades for a market.")
    parser.add_argument("ticker", help="Market ticker, e.g. KXMAYORNYCPARTY-25-D")
    parser.add_argument("--limit", type=int, default=1000, help="Page size (max 1000)")
    args = parser.parse_args()
    collect(args.ticker, limit=args.limit)


if __name__ == "__main__":
    main()
