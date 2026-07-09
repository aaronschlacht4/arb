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

NOTE (verified empirically): `enrichedOrderFilled` is NOT deduped — it includes
both the per-maker fills AND the aggregate taker-order rows (taker == Exchange).
The aggregate rows are dropped in the SEPARATE cleaning step.

ENDPOINT: defaults to the public Goldsky endpoint documented in
notes/polymarket_validation.md. Override with POLYMARKET_SUBGRAPH_URL in .env.

Token ids + output path come from events/<slug>/event.json.

Run:
    python src/fetch_polymarket_subgraph.py mamdani-dem-nomination
"""

import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from eventlib import load_event

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

PAGE = 50  # small pages: the `market` filter is weakly indexed and larger
           # pages trip the subgraph's Postgres statement-timeout for some markets.

QUERY = """
query($market: String!, $lastId: String!, $first: Int!) {
  enrichedOrderFilleds(
    first: $first
    orderBy: id
    orderDirection: asc
    where: { market: $market, id_gt: $lastId }
  ) {
    id
    transactionHash
    timestamp
    price
    size
    side
    market
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


def gql(url: str, market: str, last_id: str, tries: int = 8) -> list:
    """One page of enrichedOrderFilled, with retry/backoff on transient errors."""
    body = json.dumps({"query": QUERY,
                       "variables": {"market": market, "lastId": last_id,
                                     "first": PAGE}}).encode()
    last_err = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, data=body, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=60) as resp:
                out = json.loads(resp.read().decode("utf-8"))
            if "errors" in out:
                msg = str(out["errors"]).lower()
                if "timeout" in msg or "timed out" in msg:
                    last_err = RuntimeError(msg)
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
    raise RuntimeError(f"subgraph page failed after {tries} tries: {last_err}")


def load_ckpt(ckpt_path: Path, token_names) -> dict:
    if ckpt_path.exists():
        return json.loads(ckpt_path.read_text())
    return {"last_id": {name: "" for name in token_names}, "written": 0}


def main() -> None:
    ev = load_event(sys.argv[1] if len(sys.argv) > 1 else None)
    if len(ev.poly_token_ids) < 2:
        raise SystemExit("event.json needs polymarket_token_ids: [YES, NO]")
    tokens = {"YES": ev.poly_token_ids[0], "NO": ev.poly_token_ids[1]}
    out_path = ev.poly_subgraph_raw_jsonl
    ckpt_path = out_path.with_suffix(".checkpoint.json")

    url = load_endpoint()
    print(f"event: {ev.slug}  condition: {ev.poly_condition_id}")
    print(f"endpoint: {url.split('/subgraphs/')[0]}/subgraphs/...  "
          "(set POLYMARKET_SUBGRAPH_URL to override)")

    ck = load_ckpt(ckpt_path, tokens)
    mode = "a" if ckpt_path.exists() else "w"  # resume appends; fresh truncates
    written = ck["written"]
    t0 = time.time()

    with out_path.open(mode, encoding="utf-8") as f:
        for name, market in tokens.items():
            last = ck["last_id"][name]
            while True:
                rows = gql(url, market, last)
                if not rows:
                    break
                for r in rows:
                    # This subgraph omits the `market` field in responses, so
                    # stamp each record with the token id we queried it under
                    # (that's the outcome-token id the cleaner maps to YES/NO).
                    r.setdefault("market", market)
                    f.write(json.dumps(r) + "\n")
                written += len(rows)
                last = rows[-1]["id"]
                ck["last_id"][name] = last
                ck["written"] = written
                f.flush()
                ckpt_path.write_text(json.dumps(ck))
                rate = written / max(1e-9, time.time() - t0)
                print(f"  {name}: +{len(rows)}  total={written:,}  ({rate:.0f}/s)")
                if len(rows) < PAGE:
                    break

    print(f"\nDone. {written:,} enrichedOrderFilled records -> {out_path}")


if __name__ == "__main__":
    main()
