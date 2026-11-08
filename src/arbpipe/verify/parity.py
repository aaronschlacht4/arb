"""
Parity with Wendy's run: are our scripts the same, and where does our data differ?

TWO MODES

  1. SCRIPTS (--her-raw). Feed HER raw inputs through OUR pipeline and require
     every output file to be byte-identical to hers.

         python arb.py parity mamdani-dem-nomination --her wendysResults/mamdani-dem-nomination --her-raw

     Her raw files are converted to our raw formats in a temp directory:
       1_raw/kalshi/<ticker>.json                     -> one JSON trade per line
       1_raw/polymarket/onchain/*orderfilled_all*.csv -> OrderFilled logs
                                                         re-encoded as RPC JSONL
     then clean-kalshi, clean-poly-rpc and merge run once, and match runs once
     per window folder in her 4_arbitrage/ directory (1min, 5min, 10min, ...).
     Ticker, condition id, token ids, exchange and the [start_ts, end_ts]
     window come from OUR event.json. Exit 0 only if every file matches.

  2. DATA (default). Compare OUR event's results (our own data) with her result
     folder for the same window, keyed by trade, and report where they differ.

         python arb.py parity mamdani-dem-nomination --her wendysResults/mamdani-dem-nomination

     Kalshi trades are compared by trade id. Polymarket rows are compared by
     transaction hash with shares summed, because our Data API tape has one row
     per taker order while her on-chain tape has one row per maker fill. Then
     the matches and the locked-in value.

Both sides use the same event layout (see eventlib, Event.raw_dir). Files
compared in each window, ours -> hers:
    3_sorted/kalshi_poly_merged_sorted_for_arb.csv -> 3_sorted/, same name
    3_sorted/kalshi_sorted_for_arb.csv             -> 3_sorted/, same name
    3_sorted/poly_sorted_for_arb.csv               -> 3_sorted/, same name
    4_arbitrage/arbitrage_final.csv  -> 4_arbitrage/<window>/arbitrage_final_<window>.csv
    4_arbitrage/matched_trades.csv   -> 4_arbitrage/<window>/matched_trades_<window>.csv
"""
import argparse
import csv
import json
import re
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from arbpipe.eventlib import ROOT, load_event

SRC = ROOT / "src"
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

# ours -> hers, relative to each event folder ("{w}" is the window, e.g. 5min)
FILE_PAIRS = [
    ("3_sorted/kalshi_poly_merged_sorted_for_arb.csv",
     "3_sorted/kalshi_poly_merged_sorted_for_arb.csv"),
    ("3_sorted/kalshi_sorted_for_arb.csv", "3_sorted/kalshi_sorted_for_arb.csv"),
    ("3_sorted/poly_sorted_for_arb.csv", "3_sorted/poly_sorted_for_arb.csv"),
    ("4_arbitrage/arbitrage_final.csv", "4_arbitrage/{w}/arbitrage_final_{w}.csv"),
    ("4_arbitrage/matched_trades.csv", "4_arbitrage/{w}/matched_trades_{w}.csv"),
]


def window_folders(her: Path) -> dict[str, int]:
    """{'1min': 60, '5min': 300, ...} from her 4_arbitrage/ directory."""
    out = {}
    for d in sorted((her / "4_arbitrage").iterdir()):
        m = re.fullmatch(r"(\d+)min", d.name)
        if d.is_dir() and m:
            out[d.name] = int(m.group(1)) * 60
    if not out:
        raise SystemExit(f"No <N>min folders under {her / '4_arbitrage'}")
    return dict(sorted(out.items(), key=lambda kv: kv[1]))


# --------------------------------------------------------------------------
# raw conversion (mode 1)
# --------------------------------------------------------------------------

def _word(n) -> str:
    return format(int(n), "064x")


def build_event_from_her_raw(her: Path, base_cfg: dict, out: Path) -> Path:
    """Write event.json + 1_raw/ under `out` from her raw files; return event.json."""
    path = out / "event.json"
    cfg = dict(base_cfg, slug="wendy-parity")
    path.write_text(json.dumps(cfg, indent=2))
    ev = load_event(str(path))  # our own raw paths, so the layout has one source
    ticker = cfg["kalshi_ticker"]
    for d in (ev.kalshi_raw_dir, ev.poly_raw_dir):
        d.mkdir(parents=True, exist_ok=True)

    kalshi_json = her / "1_raw" / "kalshi" / f"{ticker}.json"
    with ev.kalshi_raw_jsonl.open("w") as f:
        for t in json.loads(kalshi_json.read_text()):
            f.write(json.dumps(t) + "\n")

    onchain = her / "1_raw" / "polymarket" / "onchain"
    fills = sorted(onchain.glob("*orderfilled_all*.csv"))
    if len(fills) != 1:
        raise SystemExit(f"Expected one *orderfilled_all*.csv in {onchain}, "
                         f"found {[p.name for p in fills]}")
    with ev.poly_rpc_raw_jsonl.open("w") as f, \
            fills[0].open(encoding="utf-8") as src:
        for r in csv.DictReader(src):
            ts = int(datetime.fromisoformat(r["blockTimestampUTC"]).timestamp())
            log = {
                "transactionHash": "0x" + r["txHash"].lower().removeprefix("0x"),
                "logIndex": hex(int(r["logIndex"])),
                "blockNumber": hex(int(r["blockNumber"])),
                "blockTimestamp": hex(ts),
                "topics": [ORDER_FILLED_TOPIC,
                           "0x" + r["orderHash"].lower().removeprefix("0x").rjust(64, "0"),
                           "0x" + r["maker"].lower()[2:].rjust(64, "0"),
                           "0x" + r["taker"].lower()[2:].rjust(64, "0")],
                "data": "0x" + "".join(_word(r[c]) for c in (
                    "makerAssetId", "takerAssetId", "makerAmountFilled_raw",
                    "takerAmountFilled_raw", "fee_raw")),
            }
            f.write(json.dumps(log) + "\n")
    return path


def run_stage(module: str, event_json: Path) -> None:
    p = subprocess.run([sys.executable, "-m", module, str(event_json)],
                       cwd=SRC, capture_output=True, text=True)
    if p.returncode != 0:
        raise SystemExit(f"{module} failed:\n{p.stdout}\n{p.stderr}")


# --------------------------------------------------------------------------
# byte comparison (mode 1)
# --------------------------------------------------------------------------

def diff_files(ours: Path, hers: Path) -> str | None:
    """None when byte-identical, else a one-paragraph description."""
    a, b = ours.read_bytes(), hers.read_bytes()
    if a == b:
        return None
    la, lb = a.decode().splitlines(), b.decode().splitlines()
    for i, (x, y) in enumerate(zip(la, lb)):
        if x != y:
            return (f"{len(la) - 1} vs {len(lb) - 1} rows; first difference at line {i + 1}:\n"
                    f"        ours: {x[:160]}\n        hers: {y[:160]}")
    return f"{len(la) - 1} vs {len(lb) - 1} rows; one file is a prefix of the other"


def compare_scripts(event: str, her: Path, keep: Path | None = None) -> dict:
    """Mode 1. Returns {(window, our_file): None | description}."""
    base = json.loads((load_event(event).dir / "event.json").read_text())
    work = Path(tempfile.mkdtemp(prefix="wendy-parity-"))
    try:
        ej = build_event_from_her_raw(her, base, work)
        for mod in ("arbpipe.clean.kalshi", "arbpipe.clean.polymarket_rpc",
                    "arbpipe.arbitrage.merge"):
            run_stage(mod, ej)
        results = {}
        for w, secs in window_folders(her).items():
            cfg = json.loads(ej.read_text())
            cfg["window_seconds"] = secs
            ej.write_text(json.dumps(cfg, indent=2))
            run_stage("arbpipe.arbitrage.match", ej)
            for o, h in FILE_PAIRS:
                results[(w, o)] = diff_files(work / o, her / h.format(w=w))
            if keep:
                for d in ("3_sorted", "4_arbitrage"):
                    shutil.copytree(work / d, keep / w / d, dirs_exist_ok=True)
        return results
    finally:
        shutil.rmtree(work, ignore_errors=True)


# --------------------------------------------------------------------------
# data comparison (mode 2)
# --------------------------------------------------------------------------

def _rows(p: Path) -> list[dict]:
    with p.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def compare_data(event: str, her: Path) -> list[str]:
    ev = load_event(event)
    secs = ev.window_seconds
    w = next((k for k, v in window_folders(her).items() if v == secs), None)
    if w is None:
        raise SystemExit(f"Her run has no {secs}s window folder")
    R, H = ev.dir, her
    out = [f"ours: {R}", f"hers: {H}  ({w} window)", ""]

    # Kalshi, by trade id
    ok = {r["kalshi_trade_id"]: r for r in _rows(R / "3_sorted/kalshi_sorted_for_arb.csv")}
    hk = {r["kalshi_trade_id"]: r for r in _rows(H / "3_sorted/kalshi_sorted_for_arb.csv")}
    cols = ["kalshi_time", "kalshi_yes", "kalshi_size", "kalshi_side"]
    bad = [i for i in ok.keys() & hk.keys() if any(ok[i][c] != hk[i][c] for c in cols)]
    out += ["KALSHI (by trade id)",
            f"  ours {len(ok):,}   hers {len(hk):,}   common {len(ok.keys() & hk.keys()):,}",
            f"  only ours {len(ok.keys() - hk.keys()):,}   only hers {len(hk.keys() - ok.keys()):,}"
            f"   value mismatches {len(bad):,}", ""]

    # Polymarket, by tx with shares summed
    def by_tx(rows):
        d = defaultdict(lambda: [0, 0, set()])
        for r in rows:
            tx = r["tx"].lower().removeprefix("0x").split("#")[0]
            d[tx][0] += int(r["poly_size_orig"])
            d[tx][1] += 1
            d[tx][2].add(r["poly_time"])
        return d
    op = by_tx(_rows(R / "3_sorted/poly_sorted_for_arb.csv"))
    hp = by_tx(_rows(H / "3_sorted/poly_sorted_for_arb.csv"))
    common = op.keys() & hp.keys()
    size_bad = [t for t in common if op[t][0] != hp[t][0]]
    time_bad = [t for t in common if op[t][2] != hp[t][2]]
    out += ["POLYMARKET (by transaction, shares floored per row then summed)",
            f"  rows: ours {sum(v[1] for v in op.values()):,}   hers {sum(v[1] for v in hp.values()):,}",
            f"  shares: ours {sum(v[0] for v in op.values()):,}   hers {sum(v[0] for v in hp.values()):,}",
            f"  transactions: ours {len(op):,}   hers {len(hp):,}   common {len(common):,}",
            f"  only ours {len(op.keys() - hp.keys()):,} tx "
            f"({sum(op[t][0] for t in op.keys() - hp.keys()):,} sh)   "
            f"only hers {len(hp.keys() - op.keys()):,} tx "
            f"({sum(hp[t][0] for t in hp.keys() - op.keys()):,} sh)",
            f"  common tx with different shares {len(size_bad):,} "
            f"(net {sum(op[t][0] - hp[t][0] for t in size_bad):+,} sh)   "
            f"different timestamp {len(time_bad):,}"]
    for t in sorted(size_bad, key=lambda t: -abs(op[t][0] - hp[t][0]))[:5]:
        out.append(f"    {t[:16]}…  ours {op[t][0]:,} sh in {op[t][1]} row(s)   "
                   f"hers {hp[t][0]:,} sh in {hp[t][1]} row(s)")
    out.append("")

    # Arbitrage
    oa = _rows(R / "4_arbitrage/arbitrage_final.csv")
    ha = _rows(H / f"4_arbitrage/{w}/arbitrage_final_{w}.csv")
    om = _rows(R / "4_arbitrage/matched_trades.csv")
    hm = _rows(H / f"4_arbitrage/{w}/matched_trades_{w}.csv")
    okm = {r["trade_id"] for r in om if r["market"] == "kalshi"}
    hkm = {r["trade_id"] for r in hm if r["market"] == "kalshi"}
    lo, li = (float(x[-1]["locked_in_value"]) if x else 0.0 for x in (oa, ha))
    out += [f"ARBITRAGE ({w})",
            f"  matches: ours {len(oa):,}   hers {len(ha):,}",
            f"  contracts: ours {sum(int(r['arb_size']) for r in oa):,}   "
            f"hers {sum(int(r['arb_size']) for r in ha):,}",
            f"  Kalshi trades used: ours {len(okm):,}   hers {len(hkm):,}   "
            f"only ours {len(okm - hkm):,}   only hers {len(hkm - okm):,}",
            f"  locked-in: ours ${lo:,.3f}   hers ${li:,.3f}   diff ${lo - li:+,.3f} "
            f"({(lo - li) / li:+.2%})" if li else "  locked-in: n/a"]
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("event", help="our event slug (supplies ids and window)")
    ap.add_argument("--her", required=True, type=Path,
                    help="her event folder, e.g. wendysResults/mamdani-dem-nomination")
    ap.add_argument("--her-raw", action="store_true",
                    help="mode 1: run OUR scripts on HER raw inputs, byte-compare")
    ap.add_argument("--keep", type=Path, help="mode 1: copy our outputs here per window")
    a = ap.parse_args()
    her = a.her if a.her.is_absolute() else (ROOT / a.her)

    if not a.her_raw:
        print("\n".join(compare_data(a.event, her)))
        return

    res = compare_scripts(a.event, her, a.keep)
    n_bad = 0
    for (w, f), d in res.items():
        print(f"  [{'ok' if d is None else 'DIFF'}] {w:6s} {f}")
        if d:
            n_bad += 1
            print(f"        {d}")
    print(f"\n{len(res) - n_bad}/{len(res)} files byte-identical to hers")
    sys.exit(1 if n_bad else 0)


if __name__ == "__main__":
    main()
