"""
Resolve a pair of market identifiers into an `events/<slug>/event.json` so the
rest of the pipeline can run on a NEW event without hand-editing JSON.

You give it a Kalshi ticker and a Polymarket market (condition id, market slug,
or a polymarket.com URL); it looks both up in the venues' public metadata APIs
and writes the event config, with the resolution outcome filled in when the
market has settled.

    python arb.py resolve --kalshi KXMAYORNYCPARTY-25-D \
        --poly will-zohran-mamdani-win-the-2025-nyc-mayoral-election

ENDPOINT NOTES - each verified live (2026-11-04), and each has a trap that
silently returns the WRONG market if you get it wrong:

  Kalshi market metadata:
      GET /trade-api/v2/historical/markets?tickers=<T>      <- `tickers`, PLURAL
    A settled market is NOT on the live `/markets` endpoint (404 / empty), only
    on `/historical/markets`. And the filter there is `tickers`; passing the
    SINGULAR `ticker` is silently IGNORED and you get back an arbitrary page of
    unrelated markets. Note this is the exact opposite of `/historical/trades`,
    whose filter IS the singular `ticker` (see fetch_kalshi_trades.py). The two
    endpoints disagree, so do not generalise from one to the other.
    Sizes/prices are `*_fp` / `*_dollars` fields now (volume_fp, not volume).

  Polymarket market metadata:
      GET https://clob.polymarket.com/markets/<conditionId>
    Authoritative, works for settled markets. `tokens` carries each outcome's
    token id plus a `winner` flag, which is how we derive `resolution`.

      GET https://gamma-api.polymarket.com/markets?slug=<slug>&closed=true
    Used ONLY to turn a slug into a condition id. Trap: gamma's `slug` filter
    searches only OPEN markets unless you pass `closed=true`, so a settled
    market returns `[]` and looks nonexistent. Separately, gamma IGNORES unknown
    filter params rather than erroring - `?condition_id=0x...` returns a page of
    random markets - so never trust a gamma response without reading back the
    field you filtered on.
"""

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

from arbpipe.eventlib import EVENTS_DIR

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

# The exchange address that appears as `taker` on the aggregate taker-order rows
# the Polymarket cleaner drops. neg-risk markets clear through a DIFFERENT
# exchange contract, so we pick per market rather than hardcoding one.
EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
NEG_RISK_EXCHANGE = "0xc5d563a36ae78145c45a50134d48a1215220f80a"

UA = {"User-Agent": "arb-research/0.1", "Accept": "application/json"}


def get_json(url: str, tries: int = 5):
    """GET + parse JSON, retrying transient failures (429 / 5xx / socket)."""
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                last = e
                time.sleep(1.5 * (attempt + 1))
                continue
            raise
        except (OSError, json.JSONDecodeError) as e:
            last = e
            time.sleep(1.5 * (attempt + 1))
            continue
    raise RuntimeError(f"GET failed after {tries} tries: {url}: {last}")


# --------------------------------------------------------------- Kalshi

def resolve_kalshi(ticker: str) -> dict:
    """Look up one Kalshi market. Historical first - settled markets live there."""
    for base in (f"{KALSHI_BASE}/historical/markets", f"{KALSHI_BASE}/markets"):
        url = f"{base}?{urllib.parse.urlencode({'tickers': ticker})}"
        data = get_json(url)
        for m in data.get("markets", []):
            # VERIFY the filter actually applied - a mis-named filter param is
            # ignored and returns unrelated markets (see module docstring).
            if m.get("ticker") == ticker:
                return m
    raise SystemExit(
        f"Kalshi: no market with ticker {ticker!r} on /historical/markets or "
        f"/markets. Check the ticker - it is case-sensitive.")


# ----------------------------------------------------------- Polymarket

CONDITION_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


def poly_slug_from_url(s: str) -> str:
    """Pull the market slug out of a polymarket.com URL, else return s as-is."""
    if "polymarket.com" not in s:
        return s
    parts = [p for p in urllib.parse.urlparse(s).path.split("/") if p]
    # /event/<event-slug>/<market-slug>  or  /market/<market-slug>
    return parts[-1] if parts else s


def condition_id_from_slug(slug: str) -> str:
    """Slug -> condition id via gamma, trying closed markets first."""
    for extra in ("&closed=true", ""):
        url = f"{GAMMA_BASE}/markets?slug={urllib.parse.quote(slug)}{extra}"
        for m in get_json(url) or []:
            # gamma ignores unknown params and returns the wrong market, so
            # confirm the slug we asked for is the slug we got back.
            if m.get("slug") == slug and m.get("conditionId"):
                return m["conditionId"]
    raise SystemExit(
        f"Polymarket: could not resolve slug {slug!r} to a condition id. Pass "
        f"the 0x... condition id directly, or re-check the slug in the URL.")


def resolve_poly(ident: str) -> dict:
    """Look up one Polymarket market from a condition id, slug, or URL."""
    ident = poly_slug_from_url(ident.strip())
    cid = ident if CONDITION_RE.match(ident) else condition_id_from_slug(ident)
    m = get_json(f"{CLOB_BASE}/markets/{cid}")
    if (m.get("condition_id") or "").lower() != cid.lower():
        raise SystemExit(f"Polymarket: CLOB returned a different market for {cid}")
    if len(m.get("tokens") or []) < 2:
        raise SystemExit(
            f"Polymarket: market {cid} has {len(m.get('tokens') or [])} tokens, "
            f"need 2.")
    return m


def poly_token_ids(m: dict) -> list[str]:
    """[YES_token, NO_token], ordered by each token's own `outcome` label rather
    than by list position - a market that happens to list No first would
    otherwise invert the entire tape, turning every price p into 1-p."""
    by_outcome = {(t.get("outcome") or "").strip().upper(): t["token_id"]
                  for t in m["tokens"]}
    if "YES" not in by_outcome or "NO" not in by_outcome:
        raise SystemExit(
            f"Polymarket: expected Yes/No outcomes, got {sorted(by_outcome)}. "
            f"This pipeline handles binary markets only.")
    return [by_outcome["YES"], by_outcome["NO"]]


def poly_resolution(m: dict) -> str | None:
    """YES / NO if the market settled and one token is flagged winner."""
    if not m.get("closed"):
        return None
    for t in m["tokens"]:
        if t.get("winner"):
            return (t.get("outcome") or "").strip().upper() or None
    return None


# --------------------------------------------------------------- compose

def build_event(slug: str, kalshi: dict, poly: dict, window: int,
                start_ts: int | None, end_ts: int | None,
                gzip_raw: bool, notes: str) -> dict:
    k_res = (kalshi.get("result") or "").strip().upper()
    k_res = k_res if k_res in ("YES", "NO") else None
    p_res = poly_resolution(poly)

    if k_res and p_res and k_res != p_res:
        # The venues disagree on the outcome, which nearly always means the two
        # markets are not the same question. Refuse: an arb computed across
        # mismatched markets is meaningless, not merely imprecise.
        raise SystemExit(
            f"Resolution mismatch: Kalshi says {k_res}, Polymarket says {p_res}.\n"
            f"  Kalshi:     {kalshi.get('title')} / {kalshi.get('yes_sub_title')}\n"
            f"  Polymarket: {poly.get('question')}\n"
            f"These are probably different questions - re-check the pair.")

    cfg = {
        "slug": slug,
        "name": poly.get("question") or kalshi.get("title") or slug,
        "kalshi_ticker": kalshi["ticker"],
        "polymarket_condition_id": poly["condition_id"],
        # neg-risk markets clear through a different exchange contract, and the
        # cleaner drops aggregate rows by matching `taker == exchange`. Get this
        # wrong and either nothing is dropped (volume double-counts) or real
        # fills are dropped.
        "polymarket_exchange": (NEG_RISK_EXCHANGE if poly.get("neg_risk")
                                else EXCHANGE),
        "polymarket_token_ids": poly_token_ids(poly),
        "polymarket_market_slug": poly.get("market_slug"),
        "window_seconds": window,
        "resolution": k_res or p_res,
    }
    if start_ts is not None:
        cfg["start_ts"] = start_ts
    if end_ts is not None:
        cfg["end_ts"] = end_ts
    if gzip_raw:
        cfg["poly_raw_gzip"] = True
    cfg["kalshi_title"] = kalshi.get("title")
    cfg["kalshi_yes_sub_title"] = kalshi.get("yes_sub_title")
    cfg["notes"] = notes or (
        f"Resolved {datetime.utcnow().strftime('%Y-%m-%d')} from Kalshi "
        f"/historical/markets and Polymarket CLOB /markets. Kalshi open "
        f"{kalshi.get('open_time')}, close {kalshi.get('close_time')}.")
    return cfg


def default_slug(poly: dict, kalshi: dict) -> str:
    s = poly.get("market_slug") or kalshi["ticker"].lower()
    return re.sub(r"[^a-z0-9-]+", "-", s.lower()).strip("-")[:60]


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Resolve a Kalshi ticker + Polymarket market into "
                    "events/<slug>/event.json.")
    ap.add_argument("--kalshi", required=True, help="Kalshi market ticker")
    ap.add_argument("--poly", required=True,
                    help="Polymarket condition id (0x...), market slug, or URL")
    ap.add_argument("--slug", help="event slug (default: Polymarket market slug)")
    ap.add_argument("--window", type=int, default=300,
                    help="arb match window in seconds (default 300)")
    ap.add_argument("--start-ts", type=int, default=None,
                    help="ignore trades before this unix ts in the arb step")
    ap.add_argument("--end-ts", type=int, default=None,
                    help="ignore trades after this unix ts in the arb step")
    ap.add_argument("--gzip-raw", action="store_true",
                    help="store the Polymarket raw tape gzipped (big markets)")
    ap.add_argument("--notes", default="", help="free-text note for event.json")
    ap.add_argument("--force", action="store_true",
                    help="overwrite an existing event.json")
    args = ap.parse_args()

    print(f"Resolving Kalshi {args.kalshi} ...", file=sys.stderr)
    kalshi = resolve_kalshi(args.kalshi)
    print(f"  {kalshi.get('title')}  [{kalshi.get('yes_sub_title')}]  "
          f"status={kalshi.get('status')} result={kalshi.get('result')}",
          file=sys.stderr)

    print(f"Resolving Polymarket {args.poly} ...", file=sys.stderr)
    poly = resolve_poly(args.poly)
    print(f"  {poly.get('question')}  closed={poly.get('closed')} "
          f"neg_risk={poly.get('neg_risk')}", file=sys.stderr)

    slug = args.slug or default_slug(poly, kalshi)
    cfg = build_event(slug, kalshi, poly, args.window, args.start_ts,
                      args.end_ts, args.gzip_raw, args.notes)

    out = EVENTS_DIR / slug / "event.json"
    if out.exists() and not args.force:
        raise SystemExit(f"{out} already exists - pass --force to overwrite.")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8")

    print(f"\nWrote {out}", file=sys.stderr)
    print(json.dumps(cfg, indent=2))


if __name__ == "__main__":
    main()
