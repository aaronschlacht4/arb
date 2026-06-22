"""
Minimal example: download the FIRST PAGE of Kalshi trades for one market
and print the result.

This is a learning script, not the full pipeline. It demonstrates:
  - the endpoint:        GET /markets/trades
  - the required params:  ticker, limit
  - the response shape:   {"cursor": "...", "trades": [...]}

Run:
    python src/kalshi_first_page.py KXMAYORNYCPARTY-25-D
    python src/kalshi_first_page.py                      # uses the default ticker
"""

import json
import sys
import urllib.parse
import urllib.request

# Base URL for Kalshi's election/politics markets. The trades endpoint is public
# (no authentication needed to read the trade tape).
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Default market to query. Override by passing a ticker as the first CLI argument.
DEFAULT_TICKER = "KXMAYORNYCPARTY-25-D"


def fetch_first_page(ticker: str, limit: int = 1000) -> dict:
    """
    Fetch a single page of trades for `ticker`.

    Returns the parsed JSON dict, which has two keys:
      - "trades": a list of trade objects (may be empty)
      - "cursor": a token for the NEXT page ("" means no more pages)
    """
    # Build the query string. urlencode handles escaping for us.
    params = {"ticker": ticker, "limit": limit}
    query = urllib.parse.urlencode(params)
    url = f"{BASE_URL}/markets/trades?{query}"

    print(f"GET {url}")

    # Send the request. A User-Agent header is polite and avoids some CDNs
    # rejecting the default urllib agent.
    req = urllib.request.Request(url, headers={"User-Agent": "arb-research/0.1"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        # resp.read() gives raw bytes; decode to str, then parse JSON to a dict.
        raw_bytes = resp.read()
        return json.loads(raw_bytes.decode("utf-8"))


def main() -> None:
    # Allow an optional ticker argument: argv[0] is the script name, argv[1] the ticker.
    ticker = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TICKER

    data = fetch_first_page(ticker)

    trades = data.get("trades", [])
    cursor = data.get("cursor", "")

    print(f"\nticker            : {ticker}")
    print(f"trades on page    : {len(trades)}")
    print(f"next-page cursor  : {cursor!r}  ('' means no more pages)")

    if trades:
        # Pretty-print the first trade so you can see the real schema.
        print("\nFirst trade object:")
        print(json.dumps(trades[0], indent=2))
    else:
        print("\n(no trades returned for this ticker)")


if __name__ == "__main__":
    main()
