"""
Collect RAW Polymarket on-chain trades for an event's market by scanning the
neg-risk CTF Exchange's `OrderFilled` events directly from an Alchemy Polygon
RPC node, and saving each matching log VERBATIM to JSONL.

WHY A FULL SCAN: `OrderFilled` does not index the token id (only orderHash,
maker, taker are indexed), so `eth_getLogs` cannot filter by market. We must
pull every neg-risk `OrderFilled` log in the block range and keep only those
whose makerAssetId/takerAssetId is one of THIS market's two token ids. Unlike
the subgraph path, the on-chain log carries `logIndex`, so this is the way to
get Wendy's exact `tx;logIndex` poly ids.

The scan is RESUMABLE: a checkpoint file records the last fully-scanned block,
so re-running continues where it left off and appends to the same raw file.

Run (event-driven; block range from event.json or --start/--end):
    python src/fetch_polymarket_rpc.py mamdani-dem-nomination
    python src/fetch_polymarket_rpc.py mamdani-dem-nomination --start 66069504 --end 73352394
"""

import argparse
import http.client
import json
import re
import time
import urllib.request
from pathlib import Path

from eventlib import load_event

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"


def load_rpc_url() -> str:
    for line in ENV_PATH.read_text().splitlines():
        if line.startswith("ALCHEMY_POLYGON_URL="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("ALCHEMY_POLYGON_URL not found in .env")


class _RpcError(Exception):
    def __init__(self, err):
        self.err = err
        super().__init__(str(err))


def rpc(url: str, method: str, params: list, tries: int = 8):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                       "params": params}).encode()
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, data=body,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                out = json.loads(resp.read().decode("utf-8"))
            if "error" in out:
                raise _RpcError(out["error"])
            return out["result"]
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                last = e
                time.sleep(1.0 * (attempt + 1))
                continue
            if e.code == 400:
                try:
                    b = json.loads(e.read().decode("utf-8"))
                except Exception:
                    raise
                if "error" in b:
                    raise _RpcError(b["error"])
            raise
        except _RpcError:
            raise
        except (OSError, http.client.HTTPException, json.JSONDecodeError) as e:
            last = e
            time.sleep(1.5 * (attempt + 1))
            continue
    raise RuntimeError(f"RPC {method} failed after {tries} tries: {last}")


def get_logs_adaptive(url: str, lo: int, hi: int, span: int, exchange: str):
    """Yield raw OrderFilled logs for [lo, hi], halving span on 'response too large'."""
    suggest_re = re.compile(r"\[(0x[0-9a-fA-F]+),\s*(0x[0-9a-fA-F]+)\]")
    block = lo
    while block <= hi:
        end = min(block + span - 1, hi)
        while True:
            params = [{"fromBlock": hex(block), "toBlock": hex(end),
                       "address": exchange, "topics": [ORDER_FILLED_TOPIC]}]
            try:
                logs = rpc(url, "eth_getLogs", params)
                break
            except _RpcError as e:
                msg = str(e.err)
                m = suggest_re.search(msg)
                if m and int(m.group(2), 16) >= block:
                    end = int(m.group(2), 16)
                    continue
                if end > block:
                    end = block + (end - block) // 2
                    continue
                raise
        yield block, end, logs
        width = end - block + 1
        block = end + 1
        span = min(width * 2, 100_000) if len(logs) < 2000 else max(width, 1)


def keep_if_ours(log: dict, token_hex: set) -> bool:
    data = log["data"]
    return any(tok in data for tok in token_hex)


def main() -> None:
    ap = argparse.ArgumentParser(description="Scan raw Polymarket OrderFilled logs for an event's market.")
    ap.add_argument("event", help="Event slug, e.g. mamdani-dem-nomination")
    ap.add_argument("--start", type=int, help="start block (else event.json poly_start_block)")
    ap.add_argument("--end", type=int, help="end block (else event.json poly_end_block)")
    ap.add_argument("--span", type=int, default=10_000, help="initial block chunk size")
    ap.add_argument("--suffix", default="", help="output-file suffix for parallel range workers (e.g. .part2)")
    args = ap.parse_args()

    ev = load_event(args.event)
    exchange = ev.poly_exchange
    token_hex = {f"{int(t):064x}" for t in ev.poly_token_ids}
    out_path = ev.poly_rpc_raw_jsonl
    if args.suffix:
        out_path = out_path.with_name(out_path.stem + args.suffix + ".jsonl")
    ckpt_path = out_path.with_suffix(".checkpoint.json")

    start_block = args.start or ev.cfg.get("poly_start_block")
    end_block = args.end or ev.cfg.get("poly_end_block")
    if not start_block or not end_block:
        raise SystemExit("Provide --start/--end or set poly_start_block/poly_end_block in event.json")

    url = load_rpc_url()

    # resume from checkpoint if present
    start = start_block
    if ckpt_path.exists():
        start = json.loads(ckpt_path.read_text())["next_block"]
        print(f"Resuming from checkpoint at block {start}")
    matched = sum(1 for _ in out_path.open()) if out_path.exists() else 0

    total_blocks = end_block - start_block + 1
    t0 = time.time()
    with out_path.open("a", encoding="utf-8") as f:
        for blk_lo, blk_hi, logs in get_logs_adaptive(url, start, end_block, args.span, exchange):
            for log in logs:
                if keep_if_ours(log, token_hex):
                    f.write(json.dumps(log) + "\n")
                    matched += 1
            f.flush()
            ckpt_path.write_text(json.dumps({"next_block": blk_hi + 1, "matched": matched}))
            done = blk_hi - start_block + 1
            pct = 100.0 * done / total_blocks
            rate = done / max(1e-9, time.time() - t0)
            eta_min = (total_blocks - done) / max(1e-9, rate) / 60
            print(f"  blocks {blk_lo}-{blk_hi}  ({pct:5.1f}%)  matched={matched}  ~{eta_min:.0f}min left",
                  flush=True)

    print(f"\nDone. {matched} matching OrderFilled logs -> {out_path}")


if __name__ == "__main__":
    main()
