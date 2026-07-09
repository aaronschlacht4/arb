"""
Shared event configuration + path resolution for the cross-venue arbitrage
pipeline. Every stage (fetch -> clean -> merge -> arbitrage) imports this so the
same code runs on any event by pointing at its `events/<slug>/event.json`.

Usage:
    from eventlib import load_event
    ev = load_event("mamdani-nyc-mayor")   # or a path to an event.json
    ev.kalshi_ticker, ev.poly_condition_id, ev.window_seconds, ev.resolution
    ev.raw_dir, ev.clean_dir, ev.results_dir
    ev.kalshi_raw_jsonl, ev.kalshi_clean_csv
    ev.poly_rpc_raw_jsonl, ev.poly_rpc_clean_csv
    ev.merged_csv, ev.arb_final_csv, ev.matched_trades_csv

CLI convention: pass the event slug as the first argument to any stage script,
e.g.  `python src/merge_for_arb.py mamdani-nyc-mayor`. Defaults to the single
event if only one exists under events/.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVENTS_DIR = ROOT / "events"


class Event:
    def __init__(self, cfg: dict, event_dir: Path):
        self.cfg = cfg
        self.dir = event_dir
        self.slug = cfg["slug"]
        self.name = cfg.get("name", self.slug)
        self.kalshi_ticker = cfg["kalshi_ticker"]
        self.poly_condition_id = cfg["polymarket_condition_id"]
        self.poly_exchange = cfg.get("polymarket_exchange")
        self.poly_token_ids = cfg.get("polymarket_token_ids", [])
        self.poly_event_slug = cfg.get("polymarket_event_slug")
        self.window_seconds = int(cfg.get("window_seconds", 300))
        self.resolution = cfg.get("resolution")  # "YES" | "NO" | None
        # Optional inclusive start cutoff: trades with unix ts < start_ts are
        # excluded from the arbitrage match (e.g. isolate the mayoral race from
        # the primary period). None = no cutoff.
        self.start_ts = cfg.get("start_ts")

    # --- directories ---
    @property
    def raw_dir(self):
        return _ensure(self.dir / "raw")

    @property
    def clean_dir(self):
        return _ensure(self.dir / "clean")

    @property
    def results_dir(self):
        return _ensure(self.dir / "results")

    @property
    def reference_dir(self):
        return self.dir / "reference"

    # --- raw files ---
    @property
    def kalshi_raw_jsonl(self):
        return self.raw_dir / f"kalshi_trades_{self.kalshi_ticker}.jsonl"

    @property
    def poly_rpc_raw_jsonl(self):
        return self.raw_dir / \
            f"polymarket_rpc_orderfilled_{self.poly_condition_id}.jsonl"

    @property
    def poly_subgraph_raw_jsonl(self):
        return self.raw_dir / \
            f"polymarket_subgraph_orderfilled_{self.poly_condition_id}.jsonl"

    # --- clean tapes ---
    @property
    def kalshi_clean_csv(self):
        return self.clean_dir / f"kalshi_trades_{self.kalshi_ticker}.csv"

    @property
    def poly_rpc_clean_csv(self):
        return self.clean_dir / \
            f"polymarket_rpc_trades_{self.poly_condition_id}.csv"

    @property
    def poly_subgraph_clean_csv(self):
        return self.clean_dir / \
            f"polymarket_subgraph_trades_{self.poly_condition_id}.csv"

    # --- pipeline outputs ---
    @property
    def merged_csv(self):
        return self.results_dir / "merged_sorted_for_arb.csv"

    @property
    def arb_final_csv(self):
        return self.results_dir / "arbitrage_final.csv"

    @property
    def matched_trades_csv(self):
        return self.results_dir / "matched_trades.csv"


def _ensure(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def load_event(slug_or_path: str | None = None) -> Event:
    """Load an event by slug, by path to its event.json, or (if omitted) the
    single event under events/."""
    if slug_or_path is None:
        cands = sorted(p for p in EVENTS_DIR.glob("*/event.json"))
        if len(cands) != 1:
            raise SystemExit(
                f"Specify an event slug; found {len(cands)} events under "
                f"{EVENTS_DIR}")
        path = cands[0]
    else:
        p = Path(slug_or_path)
        path = p if p.suffix == ".json" else EVENTS_DIR / slug_or_path / "event.json"
    if not path.exists():
        raise SystemExit(f"No event config at {path}")
    cfg = json.loads(path.read_text())
    return Event(cfg, path.parent)


def event_from_argv(argv=None) -> Event:
    """Convenience: first CLI arg is the event slug (optional if single event)."""
    argv = sys.argv[1:] if argv is None else argv
    slug = argv[0] if argv else None
    return load_event(slug)
