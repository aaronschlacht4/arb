"""
Cross-venue arbitrage matcher (Wendy's two-pointer method), event-driven.

Reads the sorted merged tape (events/<slug>/results/merged_sorted_for_arb.csv,
produced by merge_for_arb.py) and writes:
  - arbitrage_final.csv   one row per match  (the arb pairs)
  - matched_trades.csv    one row per consumed trade

Algorithm (faithful port of arbitrage.py; see docs/arbitrage_matching_methodology.md):
  - Two-pointer, EARLIER trade drives; matches partners in [t, t+WINDOW].
  - BOTH sides deplete; first-partner greedy until active exhausted / no partner.
  - Arb when abs(poly_yes - kalshi_yes) >= EPS  (any price gap).
  - Poly sizes floored; poly consumed in (timestamp, tx, logIndex) order when the
    id is `<tx>;<logIndex>`, else (timestamp, id) for subgraph-sourced ids.

Run:
    python src/run_arbitrage.py mamdani-dem-nomination
"""
import csv
import sys

from eventlib import load_event

EPS = 1e-4


def _poly_sort_key(id_str: str):
    """(tx, logIndex) when id is '<tx>;<int>', else (id, 0) — deterministic either way."""
    if ";" in id_str:
        tx, li = id_str.rsplit(";", 1)
        if li.isdigit():
            return (tx, int(li))
    return (id_str, 0)


def load(ev):
    K, P = [], []
    cutoff = ev.start_ts
    with ev.merged_csv.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            ts = int(r["timestamp"])
            if cutoff is not None and ts < cutoff:
                continue
            yes = float(r["yes_price"])
            no = float(r["no_price"])
            size = float(r["size"])
            if r["market"] == "kalshi":
                K.append({"t": ts, "yes": yes, "no": no, "sz": size,
                          "id": r["id"], "side": r["side"], "osz": size})
            else:
                tx, li = _poly_sort_key(r["id"])
                P.append({"t": ts, "yes": yes, "no": no,
                          "sz": size, "id": r["id"], "tx": tx, "li": li,
                          "side": r["side"], "osz": size})
    K.sort(key=lambda x: (x["t"], x["id"]))
    P.sort(key=lambda x: (x["t"], x["tx"], x["li"]))
    for idx, k in enumerate(K):
        k["oi"] = idx
    for idx, p in enumerate(P):
        p["oi"] = idx
    return K, P


def match(K, P, window):
    if not K or not P:
        return set(), set(), []
    start = max(K[0]["t"], P[0]["t"])
    i = j = 0
    while i < len(K) and K[i]["t"] < start:
        i += 1
    while j < len(P) and P[j]["t"] < start:
        j += 1
    ku, pu, records = set(), set(), []

    def rec(p, k, a):
        ps, ks = ("NO", "YES") if p["yes"] > k["yes"] else ("YES", "NO")
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
        if K[i]["t"] <= P[j]["t"]:                       # active = kalshi
            k = K[i]
            we = k["t"] + window
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
        else:                                            # active = poly
            p = P[j]
            we = p["t"] + window
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


def main() -> None:
    ev = load_event(sys.argv[1] if len(sys.argv) > 1 else None)
    K, P = load(ev)
    ku, pu, records = match(K, P, ev.window_seconds)

    # arbitrage_final.csv — one row per match, in execution order
    ffields = ["poly_time", "kalshi_time", "poly_yes", "poly_no",
               "kalshi_yes", "kalshi_no", "arb_size", "poly_side", "kalshi_side"]
    with ev.arb_final_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ffields, lineterminator="\n")
        w.writeheader()
        w.writerows(records)

    # matched_trades.csv — one row per consumed trade
    out = []
    for k in K:
        if k["id"] in ku:
            out.append((k["t"], "kalshi", k["oi"], {
                "timestamp": k["t"], "market": "kalshi", "trade_id": k["id"],
                "side": k["side"], "size": f"{k['osz']:.3f}",
                "yes_price": f"{k['yes']:.3f}", "no_price": f"{k['no']:.3f}"}))
    for p in P:
        if p["id"] in pu:
            out.append((p["t"], "poly", p["oi"], {
                "timestamp": p["t"], "market": "poly", "trade_id": p["id"],
                "side": p["side"], "size": f"{p['osz']:.3f}",
                "yes_price": f"{p['yes']:.3f}", "no_price": f"{p['no']:.3f}"}))
    out.sort(key=lambda r: (r[0], r[1], r[2]))
    tfields = ["timestamp", "market", "trade_id", "side", "size",
               "yes_price", "no_price"]
    with ev.matched_trades_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=tfields, lineterminator="\n")
        w.writeheader()
        for _, _, _, d in out:
            w.writerow(d)

    npoly = sum(1 for r in out if r[1] == "poly")
    nkal = sum(1 for r in out if r[1] == "kalshi")
    print(f"Matches:        {len(records):,}  -> {ev.arb_final_csv}")
    print(f"Matched trades: {len(out):,}  (poly {npoly:,} / kalshi {nkal:,})  -> {ev.matched_trades_csv}")


if __name__ == "__main__":
    main()
