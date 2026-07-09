#!/usr/bin/env python3
import os
import numpy as np
import pandas as pd

# ================= CONFIG =================
KALSHI_CSV = "/Users/wendyliu/Documents/Prediction Markets/2026 Market Efficiency/Will Zohran Mamdani win the Democratic Primary for Mayor of New York City?/Kalshi/KXMAYORNYCNOMD-25-ZM.csv"
POLY_CSV   = "/Users/wendyliu/Documents/Prediction Markets/2026 Market Efficiency/Will Zohran Mamdani win the Democratic Primary for Mayor of New York City?/Polymarket/mamdani_dem_primary_poly_takers_only_final.csv"

FINAL_ARB_CSV      = "/Users/wendyliu/Documents/Prediction Markets/2026 Market Efficiency/Will Zohran Mamdani win the Democratic Primary for Mayor of New York City?/arbitrage_final.csv"
MATCHED_TRADES_CSV = "/Users/wendyliu/Documents/Prediction Markets/2026 Market Efficiency/Will Zohran Mamdani win the Democratic Primary for Mayor of New York City?/matched_trades.csv"

WINDOW = 300  # seconds

# ================= LOAD + PREP =================
kalshi_raw = pd.read_csv(KALSHI_CSV)
poly_raw   = pd.read_csv(POLY_CSV)

OUT_DIR = os.path.dirname(FINAL_ARB_CSV)
KALSHI_SORTED_CSV = os.path.join(OUT_DIR, "kalshi_sorted_for_arb.csv")
POLY_SORTED_CSV   = os.path.join(OUT_DIR, "poly_sorted_for_arb.csv")

# NEW: merged sorted file output
MERGED_SORTED_CSV = os.path.join(OUT_DIR, "kalshi_poly_merged_sorted_for_arb.csv")

# ================= PRE-ARB SORTING (NEW) =================
# Kalshi: sort by created_time_unix asc, then trade_id ASCII asc
kalshi_raw["_trade_id_str_sort"] = kalshi_raw["trade_id"].astype(str)
kalshi_raw = kalshi_raw.sort_values(
    ["created_time_unix", "_trade_id_str_sort"],
    ascending=[True, True],
    kind="mergesort"
).reset_index(drop=True)

# Poly: create sort_id = tx-maker-taker, sort by timestamp asc then sort_id ASCII asc
poly_raw["sort_id"] = (
    poly_raw["tx"].astype(str)
    + "-" + poly_raw["maker"].astype(str)
    + "-" + poly_raw["taker"].astype(str)
)
poly_raw["_sort_id_str_sort"] = poly_raw["sort_id"].astype(str)
poly_raw = poly_raw.sort_values(
    ["timestamp", "_sort_id_str_sort"],
    ascending=[True, True],
    kind="mergesort"
).reset_index(drop=True)

# ================= PREP DATAFRAMES (UNCHANGED LOGIC) =================
# --- Kalshi ---
kalshi = kalshi_raw[[
    "created_time_unix",
    "yes_price_dollars",
    "no_price_dollars",
    "count",
    "trade_id",
    "taker_side",
]].copy()

kalshi.columns = [
    "kalshi_time",
    "kalshi_yes",
    "kalshi_no",
    "kalshi_size",
    "kalshi_trade_id",
    "kalshi_side",
]

kalshi["_orig_idx"] = range(len(kalshi))

kalshi["_trade_id_str"] = kalshi["kalshi_trade_id"].astype(str)
kalshi = kalshi.sort_values(
    ["kalshi_time", "_trade_id_str"],
    ascending=[True, True],
    kind="mergesort"
).reset_index(drop=True)

# Remove everything before timestamp (Primary)
# kalshi = kalshi[kalshi["kalshi_time"] >= 1753243200].reset_index(drop=True)

kalshi["kalshi_size"] = kalshi["kalshi_size"].astype(float)

# Save original size for matched_trades output
kalshi["kalshi_size_orig"] = kalshi["kalshi_size"].copy()

# Price compare key (comparison only; does NOT change recorded prices)
kalshi["kalshi_yes_key"] = (kalshi["kalshi_yes"].astype(float) * 1000).round().astype(int)

# --- Poly ---
poly = poly_raw[[
    "timestamp",
    "tx",
    "logIndex",
    "outcome",
    "shares",
    "price",
]].copy()

poly["poly_time"] = poly["timestamp"]
poly["poly_size"] = poly["shares"].astype(float)

# integer-floor shares before arbitrage (your earlier request)
poly["poly_size"] = np.floor(poly["poly_size"]).astype(int)

# YES/NO price calc (same logic)
poly_yes_list = []
poly_no_list  = []
for idx in range(len(poly)):
    outcome = poly.iloc[idx]["outcome"]
    price = float(poly.iloc[idx]["price"])
    if outcome == "YES":
        yes_price = price
        no_price  = 1.0 - price
    else:
        yes_price = 1.0 - price
        no_price  = price
    poly_yes_list.append(yes_price)
    poly_no_list.append(no_price)

poly["poly_yes"] = poly_yes_list
poly["poly_no"]  = poly_no_list

poly["_orig_idx"] = range(len(poly))

# Save original size for matched_trades output
poly["poly_size_orig"] = poly["poly_size"].copy()

poly["poly_yes"] = poly["poly_yes"].astype(float).round(3)
poly["poly_no"] = poly["poly_no"].astype(float).round(3)

# Price compare key (comparison only; does NOT change recorded prices)
poly["poly_yes_key"] = (poly["poly_yes"] * 1000).round().astype(int)

# Keep only what arbitrage needs (+ orig tracking)
poly = poly[[
    "tx", "logIndex", "poly_time", "poly_yes", "poly_no", "poly_yes_key",
    "poly_size", "poly_size_orig", "outcome", "_orig_idx"
]]

# Sort by timestamp then by sort_id (ASCII)
poly["tx_str"] = poly["tx"].astype(str)
poly["log_index_int"] = poly["logIndex"].astype(int)
poly = poly.sort_values(
    ["poly_time", "tx_str", "log_index_int"],
    ascending=[True, True, True],
    kind="mergesort"
).reset_index(drop=True)


poly["id"] = poly["tx_str"] + ";" + poly["logIndex"].astype(int).astype(str)

poly = poly[[
    "tx", "logIndex", "poly_time", "poly_yes", "poly_no", "poly_yes_key",
    "poly_size", "poly_size_orig", "outcome", "_orig_idx", "id"
]]

# Remove everything before timestamp (Primary)
# poly = poly[poly["poly_time"] >= 1753243200].reset_index(drop=True)

# Output sorted dfs used for arbitrage
kalshi.drop(columns=["_trade_id_str"], inplace=True, errors="ignore")
kalshi.to_csv(KALSHI_SORTED_CSV, index=False)
poly.to_csv(POLY_SORTED_CSV, index=False)

# ================= MERGED SORTED FILE =================
# Keep: size, yes/no prices, timestamp, id, market, side
# - kalshi id = trade_id
# - poly id = sort_id
# - side = kalshi_side (for kalshi) OR outcome (for poly)

kalshi_merge = pd.DataFrame({
    "timestamp": kalshi["kalshi_time"].astype(int),
    "id": kalshi["kalshi_trade_id"].astype(str),
    "market": "kalshi",
    "side": kalshi["kalshi_side"],   # <-- Kalshi side
    "size": kalshi["kalshi_size"].astype(float),
    "yes_price": kalshi["kalshi_yes"].astype(float),
    "no_price": kalshi["kalshi_no"].astype(float),
})

poly_merge = pd.DataFrame({
    "timestamp": poly["poly_time"].astype(int),
    'id': poly["id"].astype(str),
    "market": "poly",
    "side": poly["outcome"],   # <-- Poly side comes from outcome
    "size": poly["poly_size"].astype(float),
    "yes_price": poly["poly_yes"].astype(float),
    "no_price": poly["poly_no"].astype(float),
})

merged_sorted = pd.concat([kalshi_merge, poly_merge], ignore_index=True)

merged_sorted["_id_str"] = merged_sorted["id"].astype(str)
merged_sorted = merged_sorted.sort_values(
    ["timestamp", "_id_str"],
    ascending=[True, True],
    kind="mergesort"
).reset_index(drop=True)

merged_sorted.drop(columns=["_id_str"], inplace=True)

merged_sorted.to_csv(MERGED_SORTED_CSV, index=False)

# ================= ALIGN START =================
if kalshi.empty or poly.empty:
    raise ValueError("After timestamp filter, one of the datasets is empty.")

start_time = max(
    int(kalshi["kalshi_time"].min()),
    int(poly["poly_time"].min())
)
start_time = max(int(kalshi["kalshi_time"].min()), int(poly["poly_time"].min()))

# ================= CONVERT TO LISTS =================
K = kalshi.to_dict("records")
P = poly.to_dict("records")

# Move pointers to aligned start
i = 0
while i < len(K) and K[i]["kalshi_time"] < start_time:
    i += 1

j = 0
while j < len(P) and P[j]["poly_time"] < start_time:
    j += 1

# ================= MATCHING (active trade consumes within +WINDOW) =================
records = []

kalshi_used_out_trade_id = set()
poly_used_out_ids = set()

EPS = 1e-4

while i < len(K) and j < len(P):

    # skip dead rows
    while i < len(K) and K[i]["kalshi_size"] <= 0:
        i += 1
    while j < len(P) and P[j]["poly_size"] <= 0:
        j += 1
    if i >= len(K) or j >= len(P):
        break

    # active = earlier timestamp
    if K[i]["kalshi_time"] <= P[j]["poly_time"]:
        # ================= ACTIVE = KALSHI =================
        k = K[i]
        active_time = k["kalshi_time"]
        window_end = active_time + WINDOW

        # make sure global j is not behind active_time
        while j < len(P) and P[j]["poly_time"] < active_time:
            j += 1

        # now keep matching THIS kalshi trade within its window until:
        # - kalshi exhausted, OR
        # - no arb partner exists in window
        while i < len(K) and K[i]["kalshi_size"] > 0:

            # search for a poly trade within window that arbs
            jj = j
            found = False
            while jj < len(P):
                p = P[jj]

                if p["poly_time"] > window_end:
                    break

                if p["poly_size"] > 0 and abs(p["poly_yes"] - k["kalshi_yes"]) >= EPS:
                    found = True
                    break

                jj += 1

            if not found:
                # no arb partner for this active kalshi trade in its window -> discard it
                i += 1
                break

            # execute match with P[jj]
            p = P[jj]
            arb_size = k["kalshi_size"]
            if p["poly_size"] < arb_size:
                arb_size = p["poly_size"]

            if arb_size <= 0:
                # shouldn't happen, but just in case
                if k["kalshi_size"] <= 0:
                    i += 1
                    break
                jj += 1
                continue

            # sides
            if p["poly_yes"] > k["kalshi_yes"]:
                poly_side = "NO"
                kalshi_side = "YES"
            else:
                poly_side = "YES"
                kalshi_side = "NO"

            records.append({
                "poly_time": p["poly_time"],
                "kalshi_time": k["kalshi_time"],
                "poly_yes": p["poly_yes"],
                "poly_no": p["poly_no"],
                "kalshi_yes": k["kalshi_yes"],
                "kalshi_no": k["kalshi_no"],
                "arb_size": arb_size,
                "poly_side": poly_side,
                "kalshi_side": kalshi_side,
            })

            kalshi_used_out_trade_id.add(k["kalshi_trade_id"])
            poly_used_out_ids.add(p["id"])

            # consume liquidity
            k["kalshi_size"] -= arb_size
            p["poly_size"] -= arb_size

            # if the matched poly trade at jj is now empty, that's fine;
            # we do NOT need to advance global j unless it was at the front.
            while j < len(P) and P[j]["poly_size"] <= 0:
                j += 1

            # if active kalshi exhausted, move i
            if k["kalshi_size"] <= 0:
                i += 1
                break

    else:
        # ================= ACTIVE = POLY =================
        p = P[j]
        active_time = p["poly_time"]
        window_end = active_time + WINDOW

        # make sure global i is not behind active_time
        while i < len(K) and K[i]["kalshi_time"] < active_time:
            i += 1

        # match THIS poly trade within its window until:
        # - poly exhausted, OR
        # - no arb partner exists in window
        while j < len(P) and P[j]["poly_size"] > 0:

            ii = i
            found = False
            while ii < len(K):
                k = K[ii]

                if k["kalshi_time"] > window_end:
                    break

                if k["kalshi_size"] > 0 and abs(p["poly_yes"] - k["kalshi_yes"]) >= EPS:
                    found = True
                    break

                ii += 1

            if not found:
                # no arb partner for this active poly trade in its window -> discard it
                j += 1
                break

            k = K[ii]
            arb_size = p["poly_size"]
            if k["kalshi_size"] < arb_size:
                arb_size = k["kalshi_size"]

            if arb_size <= 0:
                if p["poly_size"] <= 0:
                    j += 1
                    break
                ii += 1
                continue

            if p["poly_yes"] > k["kalshi_yes"]:
                poly_side = "NO"
                kalshi_side = "YES"
            else:
                poly_side = "YES"
                kalshi_side = "NO"

            records.append({
                "poly_time": p["poly_time"],
                "kalshi_time": k["kalshi_time"],
                "poly_yes": p["poly_yes"],
                "poly_no": p["poly_no"],
                "kalshi_yes": k["kalshi_yes"],
                "kalshi_no": k["kalshi_no"],
                "arb_size": arb_size,
                "poly_side": poly_side,
                "kalshi_side": kalshi_side,
            })

            kalshi_used_out_trade_id.add(k["kalshi_trade_id"])
            poly_used_out_ids.add(p["id"])

            k["kalshi_size"] -= arb_size
            p["poly_size"] -= arb_size

            # advance i only if front is empty
            while i < len(K) and K[i]["kalshi_size"] <= 0:
                i += 1

            # if active poly exhausted, move j
            if p["poly_size"] <= 0:
                j += 1
                break

# ================= OUTPUT 1: FINAL ARBITRAGE CSV (ONLY) =================
arb_df = pd.DataFrame(records)

# drop float arb_size + replace with int arb_size
if len(arb_df) > 0:
    arb_df["arb_size"] = arb_df["arb_size"].astype(int)

arb_df.to_csv(FINAL_ARB_CSV, index=False)

print("Done.")
print("Final arbitrage file written to:")
print(FINAL_ARB_CSV)
print("Total arb records:", len(arb_df))

print("Sorted Kalshi file written to:")
print(KALSHI_SORTED_CSV)
print("Sorted Poly file written to:")
print(POLY_SORTED_CSV)

print("Merged sorted file written to:")
print(MERGED_SORTED_CSV)

# ================= OUTPUT 2: MATCHED TRADES CSV =================
kalshi_used = kalshi.loc[kalshi["kalshi_trade_id"].isin(kalshi_used_out_trade_id)].copy()
kalshi_used_out = pd.DataFrame({
    "timestamp": kalshi_used["kalshi_time"].astype(int),
    "market": "kalshi",
    "trade_id": kalshi_used["kalshi_trade_id"],
    "side": kalshi_used["kalshi_side"],
    "size": kalshi_used["kalshi_size"],   # ORIGINAL input size (count)
    "yes_price": kalshi_used["kalshi_yes"],
    "no_price": kalshi_used["kalshi_no"],
    "_orig_idx": kalshi_used["_orig_idx"],
})

poly_used = poly.loc[poly["id"].isin(poly_used_out_ids)].copy()
poly_used_out = pd.DataFrame({
    "timestamp": poly_used["poly_time"].astype(int),
    "market": "poly",
    "trade_id": poly_used["id"],   # keep as trade_id in output
    "side": poly_used["outcome"],  # FIX: poly "side" refers to raw outcome
    "size": poly_used["poly_size"],
    "yes_price": poly_used["poly_yes"].astype(float).map(lambda x: f"{x:.3f}"),
    "no_price":  poly_used["poly_no"].astype(float).map(lambda x: f"{x:.3f}"),
    "_orig_idx": poly_used["_orig_idx"],
})

matched_trades = pd.concat([kalshi_used_out, poly_used_out], ignore_index=True)

matched_trades = matched_trades.sort_values(
    ["timestamp", "market", "_orig_idx"],
    kind="mergesort"
).reset_index(drop=True)

matched_trades = matched_trades.drop(columns=["_orig_idx"])
matched_trades["yes_price"] = pd.to_numeric(matched_trades["yes_price"], errors="coerce").round(3)
matched_trades["no_price"]  = pd.to_numeric(matched_trades["no_price"],  errors="coerce").round(3)

matched_trades.to_csv(MATCHED_TRADES_CSV, index=False, float_format="%.3f")