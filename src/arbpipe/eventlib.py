"""
Shared event configuration + path resolution for the cross-venue arbitrage
pipeline. Every stage (fetch -> clean -> merge -> arbitrage) imports this so the
same code runs on any event by pointing at its `events/<slug>/event.json`.

Usage:
    from arbpipe.eventlib import load_event
    ev = load_event("mamdani-nyc-mayor")   # or a path to an event.json
    ev.kalshi_ticker, ev.poly_condition_id, ev.window_seconds, ev.resolution
    ev.raw_dir, ev.clean_dir, ev.sorted_dir, ev.arbitrage_dir, ev.charts_dir
    ev.kalshi_raw_jsonl, ev.kalshi_clean_csv
    ev.poly_rpc_raw_jsonl, ev.poly_rpc_clean_csv
    ev.merged_csv, ev.arb_final_csv, ev.matched_trades_csv, ev.results_txt

Folder layout per event: see the comment above Event.raw_dir.

CLI convention: pass the event slug as the first argument to any stage script,
e.g.  `python arb.py merge mamdani-nyc-mayor`. Defaults to the single
event if only one exists under events/.
"""
import gzip
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EVENTS_DIR = ROOT / "events"


# --- optional gzip for bulky raw tapes -------------------------------------
# Big markets (presidential-2024: 5.1M fills, ~2.8GB of JSONL) can be stored
# gzipped. The RECORDS ARE UNCHANGED — one verbatim JSON object per line, exactly
# as received; only the container is compressed, so Rule 3 still holds. Enable
# per event with  "poly_raw_gzip": true  in event.json.

def gz(path: Path) -> Path:
    """The .gz sibling of a path (a.jsonl -> a.jsonl.gz)."""
    return path.with_name(path.name + ".gz")


def raw_path(path: Path, use_gzip: bool) -> Path:
    """The path a raw tape actually lives at, given the gzip setting."""
    return gz(path) if use_gzip else path


def write_checkpoint(path: Path, data: dict) -> None:
    """Write a checkpoint ATOMICALLY (temp file + os.replace).

    A plain write that is killed half-way leaves truncated JSON, and the next
    run then dies reading its own checkpoint — turning a recoverable
    interruption into a manual repair. os.replace is atomic on both POSIX and
    Windows, so a reader sees either the old checkpoint or the new one.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data), encoding="utf-8")
    os.replace(tmp, path)


def read_checkpoint(path: Path) -> dict | None:
    """Read a checkpoint, treating a damaged one as absent rather than fatal."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"  checkpoint unreadable ({type(e).__name__}); ignoring it",
              file=sys.stderr)
        return None


def salvage_tape(path: Path, use_gzip: bool) -> tuple[int, int]:
    """Rewrite a raw tape, keeping only records that still decode intact.

    WHY THIS EXISTS. A fetcher that is hard-killed mid-write leaves the tape
    damaged in a way that is NOT self-correcting:

      - plain JSONL: a truncated final line. Appending fuses it to the next
        record, destroying both and producing a line no cleaner can parse.
      - gzip: far worse. `GzipFile.flush()` emits Z_SYNC_FLUSH but NO gzip
        trailer, so a killed member is unterminated. Reading it raises
        EOFError, and APPENDING a fresh member to it makes the file
        undecodable from the kill point on —
        `zlib.error: invalid block type` — losing every row that follows.
        Both halves verified empirically, 2026-11-05.

    So a damaged tape must be repaired BEFORE anything appends to it. We stream
    what still decodes into a sibling file and swap it in, which is a rewrite of
    the derived container, never an edit of a record (Rule 3 holds: surviving
    records are byte-identical and nothing is invented).

    Returns (records_kept, bytes_now).
    """
    src = raw_path(path, use_gzip)
    if not src.exists():
        return 0, 0
    tmp = src.with_name(src.name + ".salvage")
    tmp.unlink(missing_ok=True)

    kept = 0
    try:
        with _open_plain_or_gz(tmp, "wt", use_gzip) as out:
            try:
                with _open_plain_or_gz(src, "rt", use_gzip) as f:
                    for line in f:
                        s = line.strip()
                        if not s:
                            continue
                        try:
                            json.loads(s)
                        except json.JSONDecodeError:
                            # Truncated/fused final record — stop here; nothing
                            # after it can be trusted either.
                            break
                        out.write(s + "\n")
                        kept += 1
            except (EOFError, OSError, UnicodeDecodeError) as e:
                # Unterminated gzip member or a bad block: keep what we read.
                print(f"  salvage: stream ended early ({type(e).__name__}) — "
                      f"kept {kept:,} intact records", file=sys.stderr)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    tmp.replace(src)
    return kept, src.stat().st_size


def _open_plain_or_gz(p: Path, mode: str, use_gzip: bool):
    """Open an EXACT path (no raw_path mapping), gzipped or not."""
    if use_gzip or p.name.endswith(".gz"):
        return gzip.open(p, mode, encoding="utf-8")
    return p.open(mode, encoding="utf-8")


def truncate_tape(path: Path, use_gzip: bool, size: int | None) -> bool:
    """Cut a tape back to `size` bytes before appending to it.

    Each page is written as its own COMPLETE unit (a closed gzip member, or a
    run of whole lines) and the resulting file size is checkpointed. Truncating
    to that offset on resume drops any partially-written page, which is what
    makes the append that follows safe — see salvage_tape for what happens
    otherwise. Returns True if it actually cut anything.
    """
    p = raw_path(path, use_gzip)
    if size is None or not p.exists():
        return False
    cur = p.stat().st_size
    if cur <= size:
        return False
    with p.open("r+b") as f:
        f.truncate(size)
    print(f"  resume: cut {cur - size:,} bytes of a partially-written page",
          file=sys.stderr)
    return True


def tape_size(path: Path, use_gzip: bool) -> int:
    p = raw_path(path, use_gzip)
    return p.stat().st_size if p.exists() else 0


def tail_json_lines(path: Path, n: int, use_gzip: bool = False) -> list[dict]:
    """The last `n` parsed JSON objects of a JSONL tape, or [] if absent.

    WHY THE FETCHERS NEED THIS. Both raw fetchers checkpoint AFTER a page:
    the stored cursor points at the next page, and the stored dedupe set holds
    the last COMPLETED page's ids. If a run dies PART-WAY THROUGH a page, some
    of that page's rows are already on disk (the write buffer flushes roughly
    every ~16 rows) but appear in no checkpoint. A resume replays that page
    from the stored cursor and, finding those rows in neither dedupe set,
    writes every one of them a SECOND time.

    Reading the tape's tail on resume closes that hole: the replayed page is at
    most `limit` rows, so the last 2*limit rows are guaranteed to cover it, and
    every row that survived to disk is matched and skipped. Bounded work and
    bounded memory whatever the tape's size.

    A trailing partial line (killed mid-write) is discarded rather than parsed.
    """
    p = raw_path(path, use_gzip)
    if not p.exists() or n <= 0:
        return []
    from collections import deque
    keep: deque = deque(maxlen=n)
    # Streaming beats seeking: a gzip member cannot be seeked from the end, and
    # the memory stays flat at `n` lines either way.
    # A damaged tape must NOT raise here: an unterminated gzip member throws
    # EOFError part-way through iteration, and this function's whole job is to
    # run on a tape that a kill just damaged. Keep what decoded and move on;
    # salvage_tape is what repairs the file itself.
    try:
        with open_raw(path, "rt", use_gzip) as f:
            for line in f:
                keep.append(line)
    except (EOFError, OSError, UnicodeDecodeError) as e:
        print(f"  tail: tape ends mid-stream ({type(e).__name__}) — using the "
              f"{len(keep)} records that decoded", file=sys.stderr)
    out = []
    for line in keep:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue   # truncated final line from an abrupt kill
    return out


def open_raw(path: Path, mode: str = "rt", use_gzip: bool = False):
    """Open a raw JSONL tape, transparently gzipped when use_gzip.

    Append mode works: gzip appends a new member, and readers transparently
    stream concatenated members — so the resumable/sharded fetch is unaffected.
    """
    p = raw_path(path, use_gzip)
    if any(c in mode for c in "wax"):
        p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix == ".gz":
        return gzip.open(p, mode, encoding="utf-8")
    return p.open(mode, encoding="utf-8")


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
        # Store the (large) Polymarket raw tape gzipped. See open_raw() above.
        self.poly_raw_gzip = bool(cfg.get("poly_raw_gzip", False))
        self.window_seconds = int(cfg.get("window_seconds", 300))
        self.resolution = cfg.get("resolution")  # "YES" | "NO" | None
        # Optional cutoffs: trades with ts < start_ts or ts > end_ts are
        # excluded from the arbitrage match (e.g. isolate the mayoral race from
        # the primary period; exclude post-result price chaos). None = no cutoff.
        self.start_ts = cfg.get("start_ts")
        self.end_ts = cfg.get("end_ts")

    # --- directories ---
    # One folder per pipeline stage, numbered in the order the data flows, with
    # the two venues side by side inside the stages that are per venue:
    #
    #   events/<slug>/
    #     event.json
    #     1_raw/kalshi/  1_raw/polymarket/      as fetched, never modified
    #     2_clean/kalshi/  2_clean/polymarket/  one trade per row, shared schema
    #     3_sorted/                             per-venue + merged matcher input
    #     4_arbitrage/                          arb pairs, matched trades, results.txt
    #     5_charts/                             charts.html + PNGs
    #     reference/                            the original study's files, if any
    #
    # 1_raw and 2_clean are gitignored (large, regenerable); 3-5 are versioned.
    #
    # 1_raw/ and 2_clean/ are NOT created on access: stages probe for tapes
    # that may not exist (poly_clean_csv() checks three sources), and creating
    # folders on every probe leaves empty ones behind. The fetchers and
    # cleaners create the parent folder when they write.
    @property
    def raw_dir(self):
        return self.dir / "1_raw"

    @property
    def clean_dir(self):
        return self.dir / "2_clean"

    @property
    def kalshi_raw_dir(self):
        return self.raw_dir / "kalshi"

    @property
    def poly_raw_dir(self):
        return self.raw_dir / "polymarket"

    @property
    def kalshi_clean_dir(self):
        return self.clean_dir / "kalshi"

    @property
    def poly_clean_dir(self):
        return self.clean_dir / "polymarket"

    @property
    def sorted_dir(self):
        return _ensure(self.dir / "3_sorted")

    @property
    def arbitrage_dir(self):
        return _ensure(self.dir / "4_arbitrage")

    @property
    def charts_dir(self):
        return _ensure(self.dir / "5_charts")

    @property
    def reference_dir(self):
        return self.dir / "reference"

    # --- raw files ---
    @property
    def kalshi_raw_jsonl(self):
        return self.kalshi_raw_dir / f"kalshi_trades_{self.kalshi_ticker}.jsonl"

    @property
    def poly_rpc_raw_jsonl(self):
        return self.poly_raw_dir / \
            f"polymarket_rpc_orderfilled_{self.poly_condition_id}.jsonl"

    @property
    def poly_subgraph_raw_jsonl(self):
        return self.poly_raw_dir / \
            f"polymarket_subgraph_orderfilled_{self.poly_condition_id}.jsonl"

    @property
    def poly_dataapi_raw_jsonl(self):
        # Data API v2 — the live source. The subgraph that fed
        # poly_subgraph_raw_jsonl was deprecated in Polymarket's V2 migration.
        return self.poly_raw_dir / \
            f"polymarket_dataapi_trades_{self.poly_condition_id}.jsonl"

    # --- clean tapes ---
    @property
    def kalshi_clean_csv(self):
        return self.kalshi_clean_dir / f"kalshi_trades_{self.kalshi_ticker}.csv"

    @property
    def poly_rpc_clean_csv(self):
        return self.poly_clean_dir / \
            f"polymarket_rpc_trades_{self.poly_condition_id}.csv"

    @property
    def poly_subgraph_clean_csv(self):
        return self.poly_clean_dir / \
            f"polymarket_subgraph_trades_{self.poly_condition_id}.csv"

    @property
    def poly_dataapi_clean_csv(self):
        return self.poly_clean_dir / \
            f"polymarket_dataapi_trades_{self.poly_condition_id}.csv"

    def poly_clean_csv(self):
        """The Polymarket clean tape to use. Returns None if the event has none.

        Preference order, highest first, and why:
          1. rpc      — the on-chain scan. The only source with a true
                        logIndex, which fixes within-second consumption order,
                        so it wins wherever such a tape already exists.
          2. dataapi  — Data API v2. The only source that still WORKS for a new
                        fetch, after Polymarket's V2 migration (2026-11).
          3. subgraph — the deprecated Goldsky tape. Last, but still read, so
                        events collected before the migration keep reproducing
                        without re-fetching from a dead endpoint.

        Preferring an existing tape over a re-fetch is deliberate: it keeps the
        archived events byte-reproducible. New events only ever have (2).
        """
        for p in (self.poly_rpc_clean_csv, self.poly_dataapi_clean_csv,
                  self.poly_subgraph_clean_csv):
            if p.exists():
                return p
        return None

    # --- 3_sorted: the matcher's input ---
    # Per-venue sorted tapes in Wendy's schema (see arbitrage/merge.py), plus
    # the merged file that interleaves them.
    @property
    def kalshi_sorted_csv(self):
        return self.sorted_dir / "kalshi_sorted_for_arb.csv"

    @property
    def poly_sorted_csv(self):
        return self.sorted_dir / "poly_sorted_for_arb.csv"

    @property
    def merged_csv(self):
        return self.sorted_dir / "merged_sorted_for_arb.csv"

    # Alias: Wendy calls the merged file `kalshi_poly_merged_sorted_for_arb.csv`.
    # Same content as merged_csv.
    @property
    def merged_sorted_alias_csv(self):
        return self.sorted_dir / "kalshi_poly_merged_sorted_for_arb.csv"

    # --- 4_arbitrage: what the matcher produced ---
    @property
    def arb_final_csv(self):
        return self.arbitrage_dir / "arbitrage_final.csv"

    @property
    def matched_trades_csv(self):
        return self.arbitrage_dir / "matched_trades.csv"

    @property
    def results_txt(self):
        return self.arbitrage_dir / "results.txt"

    # --- 5_charts ---
    @property
    def charts_html(self):
        return self.charts_dir / "charts.html"


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
