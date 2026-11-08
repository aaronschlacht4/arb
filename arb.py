#!/usr/bin/env python
"""
SINGLE ENTRY POINT for the whole project. Run it from anywhere:

    python arb.py                         list the stages
    python arb.py run popvote-2024-v2     the whole pipeline for one event
    python arb.py run --all               every event that has an event.json
    python arb.py validate popvote-2024-v2
    python arb.py compare popvote-2024 popvote-2024-v2

A bare first argument that is not a stage name is taken as an event slug, so

    python arb.py popvote-2024-v2

is shorthand for `run popvote-2024-v2` — the common case stays short.

WHY THIS FILE EXISTS. The pipeline lives in the `arbpipe` package under `src/`,
which is the standard "src layout": it keeps the importable code out of the
project root so `events/`, `docs/` and `notes/` can never shadow a module. The
cost of that layout is that `src/` must be on `sys.path`. This launcher puts it
there, so the package needs NO `pip install` to run. If you do install it
(`pip install -e .`), `python -m arbpipe.cli` works identically from anywhere.

Each stage is also runnable on its own, which is what the orchestrator does
internally and what you want when re-running one step:

    cd src && python -m arbpipe.fetch.kalshi popvote-2024-v2
"""
import runpy
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent / "src"

# Friendly name -> module. The orchestrator (`run`) calls most of these itself;
# the individual entries are here for re-running a single stage by hand.
STAGES = {
    "run":             ("arbpipe.cli", "whole pipeline for an event (or --all)"),
    "resolve":         ("arbpipe.resolve", "market ids -> events/<slug>/event.json"),

    "fetch-kalshi":    ("arbpipe.fetch.kalshi", "Kalshi trades -> 1_raw/kalshi/"),
    "fetch-poly":      ("arbpipe.fetch.polymarket_dataapi",
                        "Polymarket Data API v2 trades -> 1_raw/polymarket/"),
    "fetch-poly-rpc":  ("arbpipe.fetch.polymarket_rpc",
                        "Polymarket via on-chain RPC (fallback source)"),

    "clean-kalshi":    ("arbpipe.clean.kalshi", "1_raw -> 2_clean/kalshi/"),
    "clean-poly":      ("arbpipe.clean.polymarket_dataapi",
                        "1_raw -> 2_clean/polymarket/"),
    "clean-poly-rpc":  ("arbpipe.clean.polymarket_rpc", "raw RPC -> 2_clean/polymarket/"),

    "validate":        ("arbpipe.verify.validate",
                        "check both tapes against independent ground truth"),
    "compare":         ("arbpipe.verify.compare",
                        "diff two collections of the SAME market pair"),
    "reference-match": ("arbpipe.verify.reference_matcher",
                        "reproduce the reference matcher, for provenance"),
    "parity":          ("arbpipe.verify.parity",
                        "byte-compare with Wendy's run (--her-raw), or diff data"),

    "merge":           ("arbpipe.arbitrage.merge",
                        "2_clean -> 3_sorted/ (4 sorted-for-arb CSVs)"),
    "match":           ("arbpipe.arbitrage.match",
                        "3_sorted -> 4_arbitrage/ (arb pairs, matched trades)"),
    "summary":         ("arbpipe.arbitrage.summary", "per-venue P&L -> 4_arbitrage/results.txt"),

    "charts":          ("arbpipe.report.charts", "5_charts/ (charts.html + 3 PNGs)"),
    "sync":            ("arbpipe.report.sync",
                        "venue synchronisation timing (needs pandas)"),
}


def usage() -> None:
    print(__doc__.strip())
    print("\nSTAGES")
    for name, (mod, desc) in STAGES.items():
        print(f"  {name:<17} {desc}")
        print(f"  {'':<17}   {mod}")
    print("\nLegacy scripts for DEAD data sources: src/arbpipe/legacy/ "
          "(see its README).")


def main() -> None:
    argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help", "help"):
        usage()
        return

    name = argv[0]
    if name in STAGES:
        module, rest = STAGES[name][0], argv[1:]
    else:
        # Not a stage — treat it as a slug (or an orchestrator flag like --all)
        # and run the full pipeline, which is what you almost always want.
        module, rest = STAGES["run"][0], argv

    # src/ on sys.path is what makes `arbpipe` importable without installing.
    sys.path.insert(0, str(SRC))
    sys.argv = [module, *rest]
    runpy.run_module(module, run_name="__main__")


if __name__ == "__main__":
    main()
