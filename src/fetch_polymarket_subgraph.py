"""
Collect RAW Polymarket trade primitives for the Mamdani NYC-mayor market via the
GraphQL SUBGRAPH (Wendy's prescribed method), instead of scanning eth_getLogs.

This is the second, independent acquisition path. It queries the Polymarket
orderbook subgraph's `enrichedOrderFilled` entity (which has an INDEXED `market`
field = the outcome-token id, plus pre-decoded price/size/side/timestamp), pages
through every fill for each of the market's two token ids, and saves each record
VERBATIM to JSONL.

WHY THIS IS FAST: unlike the raw `eth_getLogs` scan (which must read every block
because OrderFilled doesn't index the token id), the subgraph indexes `market`,
so we only fetch this market's fills — minutes, not hours.

NOTE (verified empirically): `enrichedOrderFilled` is NOT deduped — it includes
both the per-maker fills AND the aggregate taker-order rows (taker == Exchange).
Its count therefore equals `orderbook.tradesQuantity` (YES 398,505 / NO 190,104).
The aggregate rows are dropped in the SEPARATE cleaning step, exactly as in the
eth_getLogs pipeline.

ENDPOINT: defaults to the public Goldsky endpoint documented in
notes/polymarket_validation.md. To use a different (e.g. Wendy's confidential
Alchemy-hosted) subgraph, set POLYMARKET_SUBGRAPH_URL in .env — same schema.

Run:
    python src/fetch_polymarket_subgraph.py
"""

import json
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
ENV_PATH = PROJECT_ROOT / ".env"

CONDITION_ID = "0xebddfcf7b4401dade8b4031770a1ab942b01854f3bed453d5df9425cd9f211a9"
# The two outcome-token ids (= subgraph `market` ids), as decimal strings.
TOKENS = {
    "YES": "33945469250963963541781051637999677727672635213493648594066577298999471399137",
    "NO": "105832362350788616148612362642992403996714020918558917275151746177525518770551",
}

# Public, no-auth subgraph (Goldsky). Overridable via .env for a private host.
DEFAULT_SUBGRAPH_URL = (
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw"
    "/subgraphs/polymarket-orderbook-resync/prod/gn"
)

OUT_PATH = RAW_DIR / f"polymarket_subgraph_orderfilled_{CONDITION_ID}.jsonl"
CKPT_PATH = RAW_DIR / f"polymarket_subgraph_orderfilled_{CONDITION_ID}.checkpoint.json"

# Some hosts (Goldsky) 403 the default urllib User-Agent; send a browser-like one.
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}

# The `market` filter is only weakly indexed on this subgraph and the maker/taker
# joins are costly, so large pages hit the server's statement-timeout. Empirically
# first:200 with the full field set completes in ~1s; first:500+ times out. We
# paginate by `id_gt` (cursor) to avoid the subgraph's skip-depth cap.
PAGE = 200

QUERY = """
query($market: String!, $lastId: String!) {
  enrichedOrderFilleds(
    first: 200
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
    body = json.dumps({"query": QUERY, "variables": {"market": market, "lastId": last_id}}).encode()
    last_err = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, data=body, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=60) as resp:
                out = json.loads(resp.read().decode("utf-8"))
            if "errors" in out:
                msg = str(out["errors"]).lower()
                # Two distinct, load-dependent timeouts occur at this page size:
                # Postgres "statement timeout" and the gateway's "Query timed out".
                # Both are transient — back off and retry rather than die.
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


def load_ckpt() -> dict:
    if CKPT_PATH.exists():
        return json.loads(CKPT_PATH.read_text())
    return {"last_id": {name: "" for name in TOKENS}, "written": 0}


def save_ckpt(ck: dict) -> None:
    CKPT_PATH.write_text(json.dumps(ck))


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    url = load_endpoint()
    print(f"endpoint: {url.split('/subgraphs/')[0]}/subgraphs/...  (set POLYMARKET_SUBGRAPH_URL to override)")

    ck = load_ckpt()
    mode = "a" if CKPT_PATH.exists() else "w"  # resume appends; fresh truncates
    written = ck["written"]
    t0 = time.time()

    with OUT_PATH.open(mode, encoding="utf-8") as f:
        for name, market in TOKENS.items():
            last = ck["last_id"][name]
            while True:
                rows = gql(url, market, last)
                if not rows:
                    break
                for r in rows:
                    f.write(json.dumps(r) + "\n")  # verbatim subgraph record
                written += len(rows)
                last = rows[-1]["id"]
                ck["last_id"][name] = last
                ck["written"] = written
                f.flush()
                save_ckpt(ck)
                rate = written / max(1e-9, time.time() - t0)
                print(f"  {name}: +{len(rows)}  total={written:,}  ({rate:.0f}/s)")
                if len(rows) < PAGE:
                    break

    print(f"\nDone. {written:,} enrichedOrderFilled records -> {OUT_PATH}")


if __name__ == "__main__":
    main()
