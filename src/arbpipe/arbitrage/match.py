"""
Cross-venue arbitrage matcher (Wendy's two-pointer method), event-driven.

Reads the sorted merged tape (events/<slug>/3_sorted/merged_sorted_for_arb.csv,
produced by merge_for_arb.py) and writes:
  - arbitrage_final.csv   one row per match (the arb pairs), in Wendy's
                          24-column layout; see arb_rows()
  - matched_trades.csv    one row per consumed trade, ordered as hers
Both are byte-identical to Wendy's on her inputs (tests/test_wendy_parity.py).

Algorithm (faithful port of arbitrage.py; see docs/arbitrage_matching_methodology.md):
  - Two-pointer, EARLIER trade drives; matches partners in [t, t+WINDOW].
  - BOTH sides deplete; first-partner greedy until active exhausted / no partner.
  - Arb when abs(poly_yes - kalshi_yes) >= EPS  (any price gap).
  - Poly sizes floored; poly consumed in (timestamp, tx, logIndex) order when the
    id is `<tx>;<logIndex>`, else (timestamp, id) for subgraph-sourced ids.

Run:
    python arb.py match mamdani-dem-nomination
"""
import csv
import sys

from arbpipe.eventlib import load_event

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
    end = ev.end_ts
    with ev.merged_csv.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            ts = int(r["timestamp"])
            if cutoff is not None and ts < cutoff:
                continue
            if end is not None and ts > end:
                continue
            yes = float(r["yes_price"])
            no = float(r["no_price"])
            size = float(r["size"])
            if r["market"] == "kalshi":
                K.append({"t": ts, "yes": yes, "no": no, "sz": size,
                          "id": r["id"], "side": r["side"], "osz": size})
            else:
                tx, li = _poly_sort_key(r["id"])
                # 3dp rounding is part of the verified methodology (see merge)
                P.append({"t": ts, "yes": round(yes, 3), "no": round(no, 3),
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


def f3(v: float) -> str:
    """Wendy's 3-dp format. A tiny negative prints as "-0.000", as in hers."""
    return f"{v:.3f}"


ARB_FIELDS = [
    "poly_time", "kalshi_time", "poly_yes", "poly_no", "kalshi_yes", "kalshi_no",
    "arb_size", "poly_side", "kalshi_side",
    "poly_cash", "poly_yes_contracts", "poly_value",
    "kalshi_cash", "kalshi_yes_contracts", "kalshi_value",
    "total_poly_yes", "total_poly_no", "total_kalshi_yes", "total_kalshi_no",
    "poly_net_yes", "kalshi_net_yes", "expense", "total_net_value",
    "locked_in_value",
]


def arb_rows(records: list[dict]):
    """Walk the matches through the portfolio and yield Wendy's 24-column rows.

    Running columns, all cumulative through the row:
      poly_cash             Polymarket cash, NO booked as a YES short (y_poly)
      kalshi_cash           Kalshi cash booked in NO terms instead: a NO buy
                            pays size * kalshi_no, a YES buy is a NO short
                            credited size * kalshi_no. Equals y_kalshi + z_kalshi.
      *_yes_contracts       net YES-equivalent position (z); same as *_net_yes
      poly_value            y_poly + z_poly * poly_yes on this row
      kalshi_value          kalshi_cash + NO contracts * kalshi_no on this row.
                            Mathematically y_kalshi + z_kalshi * kalshi_yes,
                            but computed in NO terms, as Wendy does, so the
                            float residues (0.000 vs -0.000) match hers.
    Both conventions were verified on every row of her 1, 5 and 10 minute files.
      total_<venue>_<side>  contracts bought of that side
      expense               minus total cash paid on both venues
      total_net_value       poly_value + kalshi_value
      locked_in_value       y_poly + y_kalshi (price-invariant)
    """
    from arbpipe.portfolio import Portfolio

    pf = Portfolio()
    tot = {"py": 0.0, "pn": 0.0, "ky": 0.0, "kn": 0.0}
    spend = 0.0
    k_cash = k_no = 0.0  # Kalshi in NO terms (see docstring)
    for r in records:
        a = r["arb_size"]
        if r["poly_side"] == "YES":
            pf.poly.buy_yes(a, r["poly_yes"]); tot["py"] += a; spend += a * r["poly_yes"]
        else:
            pf.poly.buy_no(a, r["poly_no"]); tot["pn"] += a; spend += a * r["poly_no"]
        if r["kalshi_side"] == "YES":
            pf.kalshi.buy_yes(a, r["kalshi_yes"]); tot["ky"] += a; spend += a * r["kalshi_yes"]
            k_cash += a * r["kalshi_no"]; k_no -= a
        else:
            pf.kalshi.buy_no(a, r["kalshi_no"]); tot["kn"] += a; spend += a * r["kalshi_no"]
            k_cash -= a * r["kalshi_no"]; k_no += a
        pv = pf.poly.value(r["poly_yes"])
        kv = k_cash + k_no * r["kalshi_no"]
        yield [
            r["poly_time"], r["kalshi_time"],
            f3(r["poly_yes"]), f3(r["poly_no"]), f3(r["kalshi_yes"]), f3(r["kalshi_no"]),
            a, r["poly_side"], r["kalshi_side"],
            f3(pf.poly.y), f3(pf.poly.z), f3(pv),
            f3(k_cash), f3(pf.kalshi.z), f3(kv),
            f3(tot["py"]), f3(tot["pn"]), f3(tot["ky"]), f3(tot["kn"]),
            f3(pf.poly.z), f3(pf.kalshi.z), f3(-spend), f3(pv + kv),
            f3(pf.locked_in_value),
        ]


def orig_idx_maps(ev) -> tuple[dict, dict]:
    """Wendy's _orig_idx per trade id, from merge's per-venue files.

    matched_trades.csv is ordered (timestamp, market, _orig_idx), as hers is.
    Returns empty maps when the files are missing; callers then fall back to
    the matcher's own index.
    """
    maps = []
    for path, id_cols in ((ev.kalshi_sorted_csv, ("kalshi_trade_id", "id")),
                          (ev.poly_sorted_csv, ("id",))):
        m = {}
        if path.exists():
            with path.open(encoding="utf-8") as f:
                rd = csv.DictReader(f)
                col = next((c for c in id_cols if c in (rd.fieldnames or [])), None)
                if col and "_orig_idx" in rd.fieldnames:
                    m = {r[col]: int(r["_orig_idx"]) for r in rd}
        maps.append(m)
    return maps[0], maps[1]


def main() -> None:
    ev = load_event(sys.argv[1] if len(sys.argv) > 1 else None)
    K, P = load(ev)
    ku, pu, records = match(K, P, ev.window_seconds)

    # arbitrage_final.csv — one row per match, in execution order
    with ev.arb_final_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(ARB_FIELDS)
        w.writerows(arb_rows(records))

    # matched_trades.csv — one row per consumed trade
    k_oi, p_oi = orig_idx_maps(ev)
    out = []
    for k in K:
        if k["id"] in ku:
            out.append((k["t"], "kalshi", k_oi.get(k["id"], k["oi"]), {
                "timestamp": k["t"], "market": "kalshi", "trade_id": k["id"],
                "side": k["side"], "size": f"{k['osz']:.3f}",
                "yes_price": f"{k['yes']:.3f}", "no_price": f"{k['no']:.3f}"}))
    for p in P:
        if p["id"] in pu:
            out.append((p["t"], "poly", p_oi.get(p["id"], p["oi"]), {
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
