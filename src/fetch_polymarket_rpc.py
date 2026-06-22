"""
Collect RAW Polymarket on-chain trades for the Mamdani NYC-mayor market by
scanning the neg-risk CTF Exchange's `OrderFilled` events directly from an
Alchemy Polygon RPC node, and saving each matching log VERBATIM to JSONL.

WHY A FULL SCAN: `OrderFilled` does not index the token id (only orderHash,
maker, taker are indexed), so `eth_getLogs` cannot filter by market. We must
pull every neg-risk `OrderFilled` log in the block range and keep only those
whose makerAssetId/takerAssetId is one of THIS market's two token ids.

Everything below was verified empirically (see project notes), not assumed:
  - neg-risk Exchange : 0xc5d563a36ae78145c45a50134d48a1215220f80a
  - OrderFilled topic0: 0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6
  - data layout (5 words): makerAssetId, takerAssetId, makerAmountFilled,
                           takerAmountFilled, fee   (USDC assetId == 0)

The scan is RESUMABLE: a checkpoint file records the last fully-scanned block,
so re-running continues where it left off and appends to the same raw file.

Run:
    python src/fetch_polymarket_rpc.py
    python src/fetch_polymarket_rpc.py --start 70593581 --end 78727568
"""

import argparse
import http.client
import json
import time
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
ENV_PATH = PROJECT_ROOT / ".env"

# --- Market / contract constants (all empirically verified) ---
CONDITION_ID = "0xebddfcf7b4401dade8b4031770a1ab942b01854f3bed453d5df9425cd9f211a9"
EXCHANGE = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
YES_TOKEN = 33945469250963963541781051637999677727672635213493648594066577298999471399137
NO_TOKEN = 105832362350788616148612362642992403996714020918558917275151746177525518770551
# The two token ids rendered as 32-byte hex words, for fast substring matching
# against a log's `data` field (which is a hex string of 5 concatenated words).
TOKEN_HEX = {f"{YES_TOKEN:064x}", f"{NO_TOKEN:064x}"}

# Default block range = market life (2025-04-22 .. 2025-11-08), found by
# binary-searching block timestamps.
DEFAULT_START_BLOCK = 70_593_581
DEFAULT_END_BLOCK = 78_727_568

OUT_PATH = RAW_DIR / f"polymarket_rpc_orderfilled_{CONDITION_ID}.jsonl"
CKPT_PATH = RAW_DIR / f"polymarket_rpc_orderfilled_{CONDITION_ID}.checkpoint.json"


def load_rpc_url() -> str:
    """Read the Alchemy URL from .env (kept out of git)."""
    for line in ENV_PATH.read_text().splitlines():
        if line.startswith("ALCHEMY_POLYGON_URL="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("ALCHEMY_POLYGON_URL not found in .env")


def rpc(url: str, method: str, params: list, tries: int = 8):
    """One JSON-RPC call with retry/backoff. Returns the `result` field."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                out = json.loads(resp.read().decode("utf-8"))
            if "error" in out:
                # Alchemy signals "range too big" via an error; the caller handles it.
                raise _RpcError(out["error"])
            return out["result"]
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                last = e
                time.sleep(1.0 * (attempt + 1))
                continue
            if e.code == 400:
                # Alchemy returns JSON-RPC errors (e.g. "response size exceeded")
                # as HTTP 400 with the error object in the body. Surface it so the
                # caller's adaptive chunking can react instead of crashing.
                try:
                    body = json.loads(e.read().decode("utf-8"))
                except Exception:
                    raise
                if "error" in body:
                    raise _RpcError(body["error"])
            raise
        except _RpcError:
            raise  # response-size etc. — handled by the adaptive chunker, not here
        except (OSError, http.client.HTTPException, json.JSONDecodeError) as e:
            # All transient: read timeout, connection reset, DNS blip (OSError);
            # truncated/incomplete response (http.client.IncompleteRead); or a
            # partial body that won't parse (JSONDecodeError). Back off and retry.
            last = e
            time.sleep(1.5 * (attempt + 1))
            continue
    raise RuntimeError(f"RPC {method} failed after {tries} tries: {last}")


class _RpcError(Exception):
    """Wraps a JSON-RPC error object so we can inspect its message."""
    def __init__(self, err):
        self.err = err
        super().__init__(str(err))


def get_logs_adaptive(url: str, lo: int, hi: int, span: int):
    """
    Yield raw logs for [lo, hi] in chunks of `span` blocks, automatically
    halving `span` whenever Alchemy says the response is too large.

    This is how we stay under Alchemy's 10k-logs-per-response cap without
    knowing the density in advance.
    """
    import re
    suggest_re = re.compile(r"\[(0x[0-9a-fA-F]+),\s*(0x[0-9a-fA-F]+)\]")
    block = lo
    while block <= hi:
        end = min(block + span - 1, hi)
        # Inner loop: keep shrinking `end` until the request fits Alchemy's limit.
        while True:
            params = [{
                "fromBlock": hex(block),
                "toBlock": hex(end),
                "address": EXCHANGE,
                "topics": [ORDER_FILLED_TOPIC],
            }]
            try:
                logs = rpc(url, "eth_getLogs", params)
                break
            except _RpcError as e:
                msg = str(e.err)
                m = suggest_re.search(msg)
                if m and int(m.group(2), 16) >= block:
                    # Alchemy tells us a range that WILL fit — use it directly.
                    end = int(m.group(2), 16)
                    continue
                if end > block:
                    end = block + (end - block) // 2  # fallback: halve the window
                    continue
                raise  # single block already too big — unrecoverable
        yield block, end, logs
        width = end - block + 1
        block = end + 1
        # Adapt next window: grow when sparse, otherwise reuse the width that fit.
        span = min(width * 2, 100_000) if len(logs) < 2000 else max(width, 1)


def keep_if_ours(log: dict) -> bool:
    """True iff this OrderFilled log references one of our two token ids."""
    data = log["data"]  # hex string of 5 x 32-byte words (makerAssetId first)
    return any(tok in data for tok in TOKEN_HEX)


def load_checkpoint(default_start: int) -> int:
    if CKPT_PATH.exists():
        ck = json.loads(CKPT_PATH.read_text())
        return ck["next_block"]
    return default_start


def save_checkpoint(next_block: int, matched: int):
    CKPT_PATH.write_text(json.dumps({"next_block": next_block, "matched": matched}))


def main() -> None:
    ap = argparse.ArgumentParser(description="Scan raw Polymarket OrderFilled logs for the Mamdani market.")
    ap.add_argument("--start", type=int, default=DEFAULT_START_BLOCK)
    ap.add_argument("--end", type=int, default=DEFAULT_END_BLOCK)
    ap.add_argument("--span", type=int, default=10_000, help="initial block chunk size")
    args = ap.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    url = load_rpc_url()

    start = load_checkpoint(args.start)  # resume if a checkpoint exists
    if start > args.start:
        print(f"Resuming from checkpoint at block {start}")

    # Count already-saved matches so the running total stays correct on resume.
    matched = sum(1 for _ in OUT_PATH.open()) if OUT_PATH.exists() else 0

    total_blocks = args.end - args.start + 1
    t0 = time.time()
    with OUT_PATH.open("a", encoding="utf-8") as f:
        for blk_lo, blk_hi, logs in get_logs_adaptive(url, start, args.end, args.span):
            for log in logs:
                if keep_if_ours(log):
                    f.write(json.dumps(log) + "\n")  # raw log, verbatim
                    matched += 1
            f.flush()
            save_checkpoint(blk_hi + 1, matched)
            done = blk_hi - args.start + 1
            pct = 100.0 * done / total_blocks
            rate = done / max(1e-9, time.time() - t0)
            eta_min = (total_blocks - done) / max(1e-9, rate) / 60
            print(f"  blocks {blk_lo}-{blk_hi}  ({pct:5.1f}%)  matched={matched}  ~{eta_min:.0f}min left")

    print(f"\nDone. {matched} matching OrderFilled logs -> {OUT_PATH}")


if __name__ == "__main__":
    main()
