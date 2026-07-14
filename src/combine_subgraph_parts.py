"""
Merge the per-shard raw Polymarket files produced by parallel
`fetch_polymarket_subgraph.py --suffix .partN` runs into the single canonical
raw JSONL the cleaner expects.

WHY SHARDS: the subgraph serves a dense stretch of tape fast but chokes on the
sparse tail (see fetch_polymarket_subgraph.fetch_token). Splitting the tape into
contiguous HALF-OPEN time ranges lets 8 workers run concurrently, and confines
each slow region to its own worker instead of stalling the whole run.

WHAT THIS DOES (and does not do):
  - Concatenates every `*.partN.jsonl` for this event's condition id.
  - DEDUPES by record `id`. Half-open shard ranges should already guarantee
    disjointness, so a duplicate means overlapping ranges or a double-run — we
    count them and report, rather than assume they can't happen.
  - Records are copied VERBATIM (Rule 3). Nothing is renamed, added or dropped.
  - Refuses to overwrite an existing canonical raw file.

It also reports each token's fill count against the subgraph's OWN
`orderbook.tradesQuantity` counter, which is computed by the indexer independently
of the fills we paged — so it catches a shard that quietly came up short.

Run:
    python src/combine_subgraph_parts.py popvote-2024
    python src/combine_subgraph_parts.py popvote-2024 --force   # replace output
"""

import argparse
import hashlib
import json
from collections import Counter

from eventlib import load_event, open_raw, raw_path
from fetch_polymarket_subgraph import load_endpoint
from validate_event import gql_orderbook


def _key(rec_id: str) -> int:
    """8-byte digest of a record id, used for dedup.

    Storing the full id strings would cost ~1.2GB on presidential-2024 (5.1M ids
    x ~240B with set overhead). A 64-bit digest holds the same dedup power at a
    fraction of that: collision odds across 5.1M keys are ~7e-7.
    """
    return int.from_bytes(hashlib.blake2b(rec_id.encode(), digest_size=8).digest(),
                          "big")


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge sharded raw subgraph parts.")
    ap.add_argument("event", nargs="?", help="Event slug, e.g. popvote-2024")
    ap.add_argument("--force", action="store_true",
                    help="overwrite the canonical raw file if it already exists")
    ap.add_argument("--allow-partial", action="store_true",
                    help="the shards cover only a TIME SLICE of the market on "
                         "purpose, so do not fail the tradesQuantity check")
    args = ap.parse_args()

    ev = load_event(args.event)
    gzp = ev.poly_raw_gzip
    base_path = ev.poly_subgraph_raw_jsonl          # plain name, for globbing
    out_actual = raw_path(base_path, gzp)           # where it really lands
    if out_actual.exists() and not args.force:
        raise SystemExit(f"Refusing to overwrite existing raw file: {out_actual}\n"
                         "Pass --force if you really mean to replace it.")

    # Shard parts may be plain or gzipped, depending on the event's setting.
    parts = sorted(base_path.parent.glob(f"{base_path.stem}.part*.jsonl")) + \
        sorted(base_path.parent.glob(f"{base_path.stem}.part*.jsonl.gz"))
    if not parts:
        raise SystemExit(f"No shard files matching {base_path.stem}.part*.jsonl[.gz]")

    token_name = {ev.poly_token_ids[0]: "YES", ev.poly_token_ids[1]: "NO"}

    seen: set[int] = set()   # 64-bit digests, not full id strings — see _key()
    per_token = Counter()
    n_dupes = n_foreign = n_read = 0

    with open_raw(base_path, "wt", gzp) as out:
        for p in parts:
            n_part = 0
            opener = (lambda q: __import__("gzip").open(q, "rt", encoding="utf-8")) \
                if p.suffix == ".gz" else (lambda q: q.open(encoding="utf-8"))
            with opener(p) as fh:
                for line in fh:
                    n_read += 1
                    rec = json.loads(line)
                    if rec["market"] not in token_name:
                        # A shard somehow captured another market's fill — never
                        # write it; this would silently corrupt the tape.
                        n_foreign += 1
                        continue
                    k = _key(rec["id"])
                    if k in seen:
                        n_dupes += 1
                        continue
                    seen.add(k)
                    per_token[rec["market"]] += 1
                    out.write(line if line.endswith("\n") else line + "\n")
                    n_part += 1
            print(f"  {p.name:>62}  +{n_part:,}")

    print(f"\nrecords read      : {n_read:,}")
    print(f"  duplicates      : {n_dupes:,}  (0 unless a prior partial was kept)")
    print(f"  foreign market  : {n_foreign:,}  (expected 0)")
    print(f"records written   : {len(seen):,}  -> {out_actual}")

    # Independent completeness check against the indexer's own counters.
    print("\ncompleteness vs subgraph orderbook.tradesQuantity:")
    url = load_endpoint()
    all_ok = True
    for tok, name in token_name.items():
        expected = int(gql_orderbook(url, tok)["tradesQuantity"])
        got = per_token[tok]
        ok = got == expected
        all_ok &= ok
        flag = "ok" if ok else f"MISSING {expected - got:,}"
        print(f"  {name}: {got:,} / {expected:,}   [{flag}]")

    if not all_ok:
        if args.allow_partial:
            print("\nPARTIAL by design (--allow-partial): this tape is a time "
                  "slice, not the full market. Do NOT use it for volume totals.")
            return
        raise SystemExit("\nINCOMPLETE — a shard came up short. Re-run the "
                         "shard covering the gap (its checkpoint will resume), "
                         "then re-combine with --force.")
    print("\nAll tokens complete.")


if __name__ == "__main__":
    main()
