#!/bin/bash
# Self-contained overnight supervisor:
#   - runs the Polymarket scan with auto-resume (from checkpoint) until it finishes
#   - then runs the cleaner automatically
#   - has an 8-hour wall-clock budget
# Everything is logged so the morning state is clear. No agent needed.

cd /Users/aaronschlacht/Desktop/arb/arb || exit 1
LOG=data/raw/scan.log
MON=data/raw/monitor.log
DEADLINE=$(( $(date +%s) + 8*3600 ))

echo "[supervisor] START $(date)" >> "$MON"

# 1) Drive the scan to completion, resuming from checkpoint on any crash.
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  if python3 -u src/fetch_polymarket_rpc.py >> "$LOG" 2>&1; then
    echo "[supervisor] SCAN COMPLETED CLEANLY $(date)" >> "$LOG"
    echo "[supervisor] scan done $(date)" >> "$MON"
    break
  fi
  echo "[supervisor] scan exited non-zero, resuming in 10s $(date)" >> "$MON"
  sleep 10
done

# 2) If the scan finished, build the clean trade-level CSV.
if grep -q "SCAN COMPLETED CLEANLY" "$LOG"; then
  echo "[supervisor] running cleaner $(date)" >> "$MON"
  python3 src/clean_polymarket_rpc.py >> "$MON" 2>&1
  echo "[supervisor] CLEANER DONE $(date)" >> "$MON"
  echo "[supervisor] ALL DONE $(date)" >> "$MON"
else
  echo "[supervisor] HIT 8H DEADLINE WITHOUT COMPLETION $(date)" >> "$MON"
fi
