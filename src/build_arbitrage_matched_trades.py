"""
Reproduce Wendy's matched_trades.csv EXACTLY (byte-for-byte on the trade set).

This is a faithful port of her arbitrage.py two-pointer matcher, run on her
pre-sorted merged input (kalshi_poly_merged_sorted_for_arb.csv). Verified to
reproduce results_to_compare/matched_trades.csv with 0 missing / 0 extra.

Her algorithm (the parts that differ from a naive kalshi-driven match):
  - TWO-POINTER, earlier-trade-drives: pointers i (kalshi) and j (poly) advance
    together; whichever trade has the earlier timestamp is the "active" driver
    and consumes the other venue's liquidity. Poly can drive kalshi, not just
    the reverse.
  - FORWARD window: the active trade matches partners in [t, t + WINDOW] only.
  - BOTH sides deplete: kalshi_size and poly_size are both drawn down; a trade
    with size <= 0 is skipped.
  - First-partner greedy: for the active trade, repeatedly take the FIRST
    opposite trade (in pointer order) with size > 0 and a price difference,
    match min(sizes), until the active trade is exhausted or no partner exists
    in its window.
  - Arb test: abs(poly_yes - kalshi_yes) >= EPS  (any price gap == arbitrage;
    poly_yes is pre-rounded to 3 dp). Sides assigned by which yes is higher.
  - Consumption order: kalshi sorted (time, trade_id str); poly sorted
    (time, tx str, logIndex int). Poly sizes floored to whole contracts.

A trade is emitted once (its full, floored original size) iff it was consumed
in >= 1 match.

Run:
    python src/build_arbitrage_matched_trades.py
"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MERGED = ROOT / "data" / "clean" / "results_to_compare" / \
    "kalshi_poly_merged_sorted_for_arb.csv"
OUT = ROOT / "data" / "clean" / "arbitrage_matched_trades.csv"
OUT_FINAL = ROOT / "data" / "clean" / "arbitrage_final.csv"

WINDOW = 300
EPS = 1e-4


def load():
    K, P = [], []
    for r in csv.DictReader(MERGED.open(encoding="utf-8")):
        ts = int(r["timestamp"])
        yes = float(r["yes_price"])
        no = float(r["no_price"])
        size = float(r["size"])
        if r["market"] == "kalshi":
            K.append({"t": ts, "yes": yes, "no": no, "sz": size,
                      "id": r["id"], "side": r["side"], "osz": size})
        else:
            tx, li = r["id"].split(";")
            P.append({"t": ts, "yes": round(yes, 3), "no": round(no, 3),
                      "sz": size, "id": r["id"], "tx": tx, "li": int(li),
                      "side": r["side"], "osz": size})
    K.sort(key=lambda x: (x["t"], x["id"]))
    P.sort(key=lambda x: (x["t"], x["tx"], x["li"]))
    for idx, k in enumerate(K):
        k["oi"] = idx
    for idx, p in enumerate(P):
        p["oi"] = idx
    return K, P


def match(K, P):
    """Faithful port of arbitrage.py's two-pointer loop; returns used sets."""
    if not K or not P:
        return set(), set()
    start = max(K[0]["t"], P[0]["t"])
    i = j = 0
    while i < len(K) and K[i]["t"] < start:
        i += 1
    while j < len(P) and P[j]["t"] < start:
        j += 1
    ku, pu = set(), set()
    records = []

    def rec(p, k, a):
        if p["yes"] > k["yes"]:
            ps, ks = "NO", "YES"
        else:
            ps, ks = "YES", "NO"
        records.append({
            "poly_time": p["t"], "kalshi_time": k["t"],
            "poly_yes": p["yes"], "poly_no": p["no"],
            "kalshi_yes": k["yes"], "kalshi_no": k["no"],
            "arb_size": int(a), "poly_side": ps, "kalshi_side": ks,
        })

    while i < len(K) and j < len(P):
        while i < len(K) and K[i]["sz"] <= 0:
            i += 1
        while j < len(P) and P[j]["sz"] <= 0:
            j += 1
        if i >= len(K) or j >= len(P):
            break
        if K[i]["t"] <= P[j]["t"]:                      # active = kalshi
            k = K[i]
            we = k["t"] + WINDOW
            while j < len(P) and P[j]["t"] < k["t"]:
                j += 1
            while i < len(K) and K[i]["sz"] > 0:
                jj = j
                found = False
                while jj < len(P):
                    p = P[jj]
                    if p["t"] > we:
                        break
                    if p["sz"] > 0 and abs(p["yes"] - k["yes"]) >= EPS:
                        found = True
                        break
                    jj += 1
                if not found:
                    i += 1
                    break
                p = P[jj]
                a = min(k["sz"], p["sz"])
                if a <= 0:
                    if k["sz"] <= 0:
                        i += 1
                        break
                    jj += 1
                    continue
                rec(p, k, a)
                ku.add(k["id"])
                pu.add(p["id"])
                k["sz"] -= a
                p["sz"] -= a
                while j < len(P) and P[j]["sz"] <= 0:
                    j += 1
                if k["sz"] <= 0:
                    i += 1
                    break
        else:                                           # active = poly
            p = P[j]
            we = p["t"] + WINDOW
            while i < len(K) and K[i]["t"] < p["t"]:
                i += 1
            while j < len(P) and P[j]["sz"] > 0:
                ii = i
                found = False
                while ii < len(K):
                    k = K[ii]
                    if k["t"] > we:
                        break
                    if k["sz"] > 0 and abs(p["yes"] - k["yes"]) >= EPS:
                        found = True
                        break
                    ii += 1
                if not found:
                    j += 1
                    break
                k = K[ii]
                a = min(p["sz"], k["sz"])
                if a <= 0:
                    if p["sz"] <= 0:
                        j += 1
                        break
                    ii += 1
                    continue
                rec(p, k, a)
                ku.add(k["id"])
                pu.add(p["id"])
                k["sz"] -= a
                p["sz"] -= a
                while i < len(K) and K[i]["sz"] <= 0:
                    i += 1
                if p["sz"] <= 0:
                    j += 1
                    break
    return ku, pu, records


def main():
    K, P = load()
    ku, pu, records = match(K, P)

    # ----- arbitrage_final.csv: one row per match, in execution order -----
    ffields = ["poly_time", "kalshi_time", "poly_yes", "poly_no",
               "kalshi_yes", "kalshi_no", "arb_size", "poly_side", "kalshi_side"]
    with OUT_FINAL.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ffields, lineterminator="\n")
        w.writeheader()
        w.writerows(records)
    print(f"Wrote {len(records):,} arb pairs -> {OUT_FINAL}")

    rows = []
    for k in K:
        if k["id"] in ku:
            rows.append((k["t"], "kalshi", k["oi"], {
                "timestamp": k["t"], "market": "kalshi", "trade_id": k["id"],
                "side": k["side"], "size": f"{k['osz']:.3f}",
                "yes_price": f"{k['yes']:.3f}", "no_price": f"{k['no']:.3f}"}))
    for p in P:
        if p["id"] in pu:
            rows.append((p["t"], "poly", p["oi"], {
                "timestamp": p["t"], "market": "poly", "trade_id": p["id"],
                "side": p["side"], "size": f"{p['osz']:.3f}",
                "yes_price": f"{p['yes']:.3f}", "no_price": f"{p['no']:.3f}"}))
    # her output order: (timestamp, market, original within-venue index)
    rows.sort(key=lambda r: (r[0], r[1], r[2]))

    fields = ["timestamp", "market", "trade_id", "side", "size",
              "yes_price", "no_price"]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        w.writeheader()
        for _, _, _, d in rows:
            w.writerow(d)

    npoly = sum(1 for r in rows if r[1] == "poly")
    nkal = sum(1 for r in rows if r[1] == "kalshi")
    print(f"Wrote {len(rows):,} matched trades -> {OUT}")
    print(f"  poly:   {npoly:,}")
    print(f"  kalshi: {nkal:,}")


if __name__ == "__main__":
    main()
