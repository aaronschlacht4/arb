"""
ONE COMMAND, WHOLE PIPELINE. Give it a pair of markets; it gives you back the
arbitrage dataset and the charts.

    python arb.py run --kalshi KXMAYORNYCPARTY-25-D \
        --poly https://polymarket.com/event/nyc-mayor/will-zohran-mamdani-win-...

    python arb.py run mamdani-dem-nomination        # re-run a known event
    python arb.py run --all                         # every event with a config

STAGES (each is the existing single-purpose script, run in order):
    resolve_market            markets          -> events/<slug>/event.json
    fetch_kalshi_trades       Kalshi API       -> raw/kalshi_trades_*.jsonl
    clean_kalshi_trades       raw              -> clean/kalshi_trades_*.csv
    fetch_polymarket_dataapi  Polymarket v2    -> raw/polymarket_dataapi_*.jsonl
    clean_polymarket_dataapi  raw              -> clean/polymarket_dataapi_*.csv
    validate_event            both tapes vs independent ground truth
    merge_for_arb             clean tapes      -> the 4 sorted-for-arb CSVs
    run_arbitrage             merged           -> arbitrage_final + matched_trades
    results_summary           arb pairs        -> results.txt
    make_charts               replay           -> charts.html + 3 PNGs

DELIVERABLES, all under events/<slug>/results/ — these are the files the
pipeline exists to produce:
    kalshi_sorted_for_arb.csv              Kalshi tape, pre-arb
    poly_sorted_for_arb.csv                Polymarket tape, pre-arb
    kalshi_poly_merged_sorted_for_arb.csv  both venues interleaved
    merged_sorted_for_arb.csv              (same content, short name)
    matched_trades.csv                     every trade the matcher consumed
    arbitrage_final.csv                    one row per matched arb pair
    results.txt                            per-venue P&L and the Net
    charts.html + prices/positions/locked_in.png

IDEMPOTENCE. A stage whose output already exists is SKIPPED, so a re-run costs
nothing and an interrupted run resumes. Two deliberate exceptions:
  - the RAW fetches refuse to overwrite an existing tape (project Rule 3), and
    resume from their own checkpoint instead;
  - `--force` re-runs the DERIVED stages (clean -> charts) but still never
    re-downloads or clobbers raw data. To re-acquire raw, delete it yourself.

VALIDATION is on by default and FAILS THE RUN. It is the only thing standing
between a silently truncated fetch and a confident, wrong P&L number — the
Polymarket API's cursor drops the market filter, which turns a truncated tape
into something that looks exactly like a complete one. Use --skip-validate only
when you already know why it fails.
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from arbpipe.eventlib import EVENTS_DIR, ROOT, load_event, raw_path

SRC = ROOT / "src"


class StageError(RuntimeError):
    pass


def run(script: str, *args: str, label: str = "") -> None:
    """Run one pipeline stage as a subprocess, streaming its output.

    `script` is a MODULE path, e.g. "arbpipe.fetch.kalshi".

    Subprocess rather than import-and-call: every stage is already a working
    CLI with its own argument handling, and a stage that dies (or is killed on
    a huge market) must not take the orchestrator down with it.
    """
    # "-m" with cwd=SRC puts src/ on sys.path, so "arbpipe.*" resolves
    # without the package needing to be pip-installed first.
    cmd = [sys.executable, "-m", script, *args]
    print(f"\n{'=' * 62}\n>> {label or script}\n{'=' * 62}", flush=True)
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=str(SRC))
    el = time.time() - t0
    if proc.returncode != 0:
        raise StageError(f"{script} exited {proc.returncode} after {el:.0f}s")
    print(f"-- {script} ok ({el:.0f}s)", flush=True)


def skip(label: str, path: Path) -> None:
    print(f"\n-- skip {label}: {path.name} already exists", flush=True)


def resolve(args) -> str:
    """Make sure an event.json exists; return its slug."""
    if args.event:
        load_event(args.event)          # raises if missing or malformed
        return args.event

    extra = []
    if args.slug:
        extra += ["--slug", args.slug]
    if args.window != 300:
        extra += ["--window", str(args.window)]
    if args.start_ts is not None:
        extra += ["--start-ts", str(args.start_ts)]
    if args.end_ts is not None:
        extra += ["--end-ts", str(args.end_ts)]
    if args.gzip_raw:
        extra += ["--gzip-raw"]
    if args.force:
        extra += ["--force"]
    run("arbpipe.resolve", "--kalshi", args.kalshi, "--poly", args.poly,
        *extra, label="resolve markets -> event.json")

    # resolve_market derives the slug when we did not supply one; find it by
    # matching the ticker rather than re-deriving the slug rule here. Several
    # events can share a Kalshi ticker (the same market re-collected under a
    # different slug), so take the one just written — the newest by mtime.
    if args.slug:
        return args.slug
    matches = []
    for p in EVENTS_DIR.glob("*/event.json"):
        try:
            cfg = json.loads(p.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if cfg.get("kalshi_ticker") == args.kalshi and cfg.get("slug"):
            matches.append((p.stat().st_mtime, cfg["slug"]))
    if not matches:
        raise StageError("resolve_market.py wrote no event.json we can find")
    return max(matches)[1]


def pipeline(slug: str, force: bool, validate: bool, charts: bool) -> dict:
    ev = load_event(slug)
    print(f"\n### {slug} — {ev.name}")

    # --- Kalshi -----------------------------------------------------------
    # A LEFTOVER CHECKPOINT MEANS THE TAPE IS PARTIAL. The fetcher deletes it
    # on clean completion, so its presence says the last run was killed
    # mid-page. Treating the file's mere existence as "done" here would hand a
    # truncated tape to the cleaner, and every number downstream would be
    # quietly wrong. Re-run instead: the fetcher resumes from the checkpoint.
    k_ckpt = ev.kalshi_raw_jsonl.with_suffix(".checkpoint.json")
    if ev.kalshi_raw_jsonl.exists() and not k_ckpt.exists():
        skip("fetch kalshi", ev.kalshi_raw_jsonl)
    else:
        if k_ckpt.exists():
            print(f"\n-- kalshi tape is partial (checkpoint present) — resuming",
                  flush=True)
        run("arbpipe.fetch.kalshi", slug, label="fetch Kalshi trades")

    if ev.kalshi_clean_csv.exists() and not force:
        skip("clean kalshi", ev.kalshi_clean_csv)
    else:
        run("arbpipe.clean.kalshi", slug, label="clean Kalshi trades")

    # --- Polymarket -------------------------------------------------------
    # An event collected BEFORE the V2 migration already has an rpc or subgraph
    # clean tape. Leave it alone: re-fetching would add a second, coarser tape
    # of the same market and silently change that event's published Net. Only
    # events with no Polymarket tape at all go to the live API.
    existing = ev.poly_clean_csv()
    if existing is not None and existing != ev.poly_dataapi_clean_csv:
        skip(f"polymarket (already has a {existing.name.split('_')[1]} tape)",
             existing)
    else:
        poly_raw = raw_path(ev.poly_dataapi_raw_jsonl, ev.poly_raw_gzip)
        ckpt = ev.poly_dataapi_raw_jsonl.with_suffix(".checkpoint.json")
        poly_done = False
        if ckpt.exists():
            try:
                poly_done = bool(json.loads(ckpt.read_text()).get("done"))
            except (OSError, json.JSONDecodeError):
                poly_done = False
        if poly_raw.exists() and poly_done:
            skip("fetch polymarket", poly_raw)
        else:
            # Safe mid-tape: the fetcher resumes from its own checkpoint.
            run("arbpipe.fetch.polymarket_dataapi", slug,
                label="fetch Polymarket trades")

        if ev.poly_dataapi_clean_csv.exists() and not force:
            skip("clean polymarket", ev.poly_dataapi_clean_csv)
        else:
            run("arbpipe.clean.polymarket_dataapi", slug,
                label="clean Polymarket trades")

    # --- validate ---------------------------------------------------------
    if validate:
        run("arbpipe.verify.validate", slug, label="validate both tapes")
    else:
        print("\n-- skip validate (--skip-validate): a truncated tape will "
              "NOT be caught", flush=True)

    # --- arbitrage --------------------------------------------------------
    run("arbpipe.arbitrage.merge", slug, label="merge -> sorted-for-arb CSVs")
    run("arbpipe.arbitrage.match", slug, label="match arbitrage pairs")
    run("arbpipe.arbitrage.summary", slug, label="results.txt")
    if charts:
        run("arbpipe.report.charts", slug, label="charts.html + PNGs")

    return manifest(load_event(slug), charts)


def manifest(ev, charts: bool) -> dict:
    """What the run produced, with sizes, so the caller can see it at a glance."""
    out = {
        "slug": ev.slug,
        "name": ev.name,
        "kalshi_ticker": ev.kalshi_ticker,
        "polymarket_condition_id": ev.poly_condition_id,
        "resolution": ev.resolution,
        "window_seconds": ev.window_seconds,
        "files": {},
    }
    wanted = [
        ("kalshi_sorted_for_arb", ev.kalshi_sorted_csv),
        ("poly_sorted_for_arb", ev.poly_sorted_csv),
        ("kalshi_poly_merged_sorted_for_arb", ev.merged_sorted_alias_csv),
        ("merged_sorted_for_arb", ev.merged_csv),
        ("matched_trades", ev.matched_trades_csv),
        ("arbitrage_final", ev.arb_final_csv),
        ("results", ev.results_dir / "results.txt"),
    ]
    if charts:
        wanted += [("charts_html", ev.results_dir / "charts.html"),
                   ("prices_png", ev.results_dir / "prices.png"),
                   ("positions_png", ev.results_dir / "positions.png"),
                   ("locked_in_png", ev.results_dir / "locked_in.png")]
    for key, p in wanted:
        if p.exists():
            out["files"][key] = {"path": str(p),
                                 "bytes": p.stat().st_size,
                                 "rows": count_rows(p)}
    rt = ev.results_dir / "results.txt"
    if rt.exists():
        for line in rt.read_text().splitlines():
            if line.startswith("Net:"):
                out["net"] = float(line.split(":", 1)[1])
    return out


def count_rows(p: Path) -> int | None:
    """Data rows in a CSV (header excluded); None for anything else."""
    if p.suffix != ".csv":
        return None
    with p.open(encoding="utf-8") as f:
        return max(0, sum(1 for _ in f) - 1)


def print_manifest(m: dict) -> None:
    print(f"\n{'=' * 62}\nDELIVERABLES — {m['slug']}\n{'=' * 62}")
    print(f"  {m['name']}")
    print(f"  Kalshi {m['kalshi_ticker']}   resolution {m['resolution']}   "
          f"window {m['window_seconds']}s")
    if "net" in m:
        print(f"  Net (upper bound): ${m['net']:,.0f}")
    print()
    if not m["files"]:
        print("  (no output files — every stage was skipped or failed)")
        return
    for k, v in m["files"].items():
        rows = f"{v['rows']:>10,} rows" if v["rows"] is not None else " " * 15
        print(f"  {k:<36}{rows}  {v['bytes'] / 1e6:>7.2f} MB")
    print(f"\n  all under: {Path(next(iter(m['files'].values()))['path']).parent}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run the whole cross-venue arbitrage pipeline for a market "
                    "pair.",
        epilog="Give --kalshi/--poly for a new pair, or an event slug to re-run.")
    ap.add_argument("event", nargs="?", help="existing event slug")
    ap.add_argument("--kalshi", help="Kalshi market ticker (new event)")
    ap.add_argument("--poly", help="Polymarket condition id, slug, or URL")
    ap.add_argument("--slug", help="event slug to create (default: poly slug)")
    ap.add_argument("--all", action="store_true",
                    help="run every event that already has an event.json")
    ap.add_argument("--window", type=int, default=300,
                    help="arb match window, seconds (default 300)")
    ap.add_argument("--start-ts", type=int, default=None)
    ap.add_argument("--end-ts", type=int, default=None)
    ap.add_argument("--gzip-raw", action="store_true",
                    help="store the Polymarket raw tape gzipped (big markets)")
    ap.add_argument("--force", action="store_true",
                    help="re-run derived stages even if outputs exist "
                         "(never re-downloads or deletes raw data)")
    ap.add_argument("--skip-validate", action="store_true",
                    help="skip the completeness checks (not recommended — "
                         "they are what catch a silently truncated fetch)")
    ap.add_argument("--no-charts", action="store_true",
                    help="skip chart generation (needs matplotlib + numpy)")
    args = ap.parse_args()

    if args.all:
        slugs = [p.parent.name for p in sorted(EVENTS_DIR.glob("*/event.json"))]
    elif args.event:
        slugs = [args.event]
    elif args.kalshi and args.poly:
        slugs = [resolve(args)]
    else:
        ap.error("pass an event slug, --all, or both --kalshi and --poly")

    results, failed = [], []
    for s in slugs:
        try:
            results.append(pipeline(s, args.force, not args.skip_validate,
                                    not args.no_charts))
        except StageError as e:
            # One bad event must not abort a --all sweep; report at the end.
            print(f"\n!! {s} FAILED: {e}", file=sys.stderr)
            failed.append((s, str(e)))

    for m in results:
        print_manifest(m)

    out = EVENTS_DIR / "last_run_manifest.json"
    out.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(f"\nmanifest -> {out}")

    if failed:
        print(f"\n{len(failed)} event(s) FAILED:", file=sys.stderr)
        for s, e in failed:
            print(f"  {s}: {e}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
