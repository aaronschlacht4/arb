#!/bin/bash
# Parallel Alchemy RPC scan for presidential-2024: split the block range into
# N disjoint shards, one fetch_polymarket_rpc.py worker per shard, each with
# its own output file (.partN) and checkpoint — so the whole thing is
# resumable per-shard: re-running this script continues where each worker
# left off (same supervisor idea as supervise_scan.sh, parallelised).
#
# Requires ALCHEMY_POLYGON_URL in arb/.env (never committed).
#
# When all shards finish, combine and clean:
#   cat events/presidential-2024/raw/polymarket_rpc_orderfilled_*.part*.jsonl \
#     > events/presidential-2024/raw/polymarket_rpc_orderfilled_0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917.jsonl
#   python src/clean_polymarket_rpc.py presidential-2024
# (row order across parts doesn't matter — the cleaner sorts.)

set -u
cd "$(dirname "$0")/.." || exit 1

EVENT=presidential-2024
START=51909481          # event.json poly_start_block
END=64053541            # event.json poly_end_block
WORKERS=10

if [ ! -f .env ] || ! grep -q "^ALCHEMY_POLYGON_URL=" .env; then
  echo "ERROR: ALCHEMY_POLYGON_URL not found in .env — create it first." >&2
  exit 1
fi

TOTAL=$(( END - START + 1 ))
CHUNK=$(( (TOTAL + WORKERS - 1) / WORKERS ))
RAWDIR=events/$EVENT/raw
mkdir -p "$RAWDIR"

echo "[shards] $TOTAL blocks / $WORKERS workers = $CHUNK blocks each"
PIDS=()
for i in $(seq 1 $WORKERS); do
  LO=$(( START + (i-1)*CHUNK ))
  HI=$(( LO + CHUNK - 1 ))
  [ "$HI" -gt "$END" ] && HI=$END
  LOG=$RAWDIR/rpc_scan_part$i.log
  echo "[shards] part$i: blocks $LO-$HI  (log: $LOG)"
  python src/fetch_polymarket_rpc.py "$EVENT" \
      --start "$LO" --end "$HI" --suffix ".part$i" >> "$LOG" 2>&1 &
  PIDS+=($!)
done

FAIL=0
for pid in "${PIDS[@]}"; do
  wait "$pid" || FAIL=$(( FAIL + 1 ))
done

if [ "$FAIL" -eq 0 ]; then
  echo "[shards] ALL $WORKERS SHARDS COMPLETE"
else
  echo "[shards] $FAIL shard(s) exited non-zero — re-run this script to resume them"
  exit 1
fi
