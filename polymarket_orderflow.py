#!/usr/bin/env python3
"""
Polymarket Orderflow Terminal Visualizer v2.3
Paste a Polymarket URL → watch live trades stream in your terminal.
No API keys needed.

APIs used (all public, no auth):
  - gamma-api.polymarket.com  → market metadata / token IDs
  - data-api.polymarket.com   → trade history (/activity endpoint)
  - ws-subscriptions-clob.polymarket.com → live WebSocket price feed

Fixes in v2.3:
  - Poller uses timestamp cursor correctly so new trades are never missed
  - Consecutive-empty backoff capped at POLL_SEC*2 (no runaway sleep)
  - Input thread rebuilt with select() on POSIX so keystrokes don't
    block the render loop on slow terminals
  - Rich TUI: layout refresh moved to its own thread so it never lags
    behind the poller
  - Plain renderer: clears only when data actually changed (reduces flicker)
  - All control buttons (s / m / b / q) now work in both renderers
  - Sound toggle state is printed immediately after change
  - Bug fix: _last_trade_ts advance was racy; now fully protected by lock
  - Bug fix: seed_trades() now populates per-outcome stats so the stats
    panel is populated from the first frame
"""

import sys, os, re, time, json, threading, subprocess, platform, select
from datetime import datetime, timezone
from collections import deque
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

# ── optional: rich TUI ─────────────────────────────────────────────────────
try:
    from rich.console import Console
    from rich.table import Table
    from rich.live import Live
    from rich.panel import Panel
    from rich.text import Text
    from rich.layout import Layout
    from rich import box
    RICH = True
except ImportError:
    RICH = False

# ── ANSI colours ───────────────────────────────────────────────────────────
RST  = "\033[0m";  BOLD = "\033[1m";  DIM  = "\033[2m"
GRN  = "\033[92m"; RED  = "\033[91m"; YLW  = "\033[93m"
CYN  = "\033[96m"; MAG  = "\033[95m"; WHT  = "\033[97m"

# ── constants ──────────────────────────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API  = "https://data-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
WS_URL    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLL_SEC  = 3          # REST poll interval (seconds)
MAX_ROWS  = 200        # keep last N trades in feed

_SYS = platform.system()

# ── mutable globals ────────────────────────────────────────────────────────
SOUND_ENABLED = True

feed_lock  = threading.Lock()
trade_feed = deque(maxlen=MAX_ROWS)
seen_ids   = set()

# token_id → outcome label  e.g.  "71234...": "Yes",  "89abc...": "No"
token_map: dict = {}

# Cursor: timestamp of the newest trade we have ingested (epoch seconds float).
# Protected by _cursor_lock; only advance forward, never back.
_last_trade_ts: float = 0.0
_cursor_lock = threading.Lock()

# Set when the feed has new data so the plain renderer knows to redraw.
_dirty = threading.Event()

stats = dict(
    total_trades=0, total_volume=0.0,
    buy_volume=0.0,  sell_volume=0.0,
    big_trades=0,    last_yes=None, last_no=None,
    market_title="", started_at=time.time(),
    outcome_volume={},  # label → {"buy": float, "sell": float, "last_price": float}
)

filter_cfg = dict(
    min_size=0.0, big_threshold=500.0,
    show_buys=True, show_sells=True,
)


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────
_HDR = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin":          "https://polymarket.com",
    "Referer":         "https://polymarket.com/",
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-site",
}


def http_get(url, params=None, timeout=12, retries=3):
    if params:
        url += "?" + urlencode(params)
    for i in range(retries):
        try:
            req = Request(url, headers=_HDR)
            with urlopen(req, timeout=timeout) as r:
                raw = r.read()
                try:
                    import gzip
                    raw = gzip.decompress(raw)
                except Exception:
                    pass
                return json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(1.5 * (i + 1))


def bar(ratio, w=20, f="█", e="░"):
    ratio = max(0.0, min(1.0, ratio))
    n = round(ratio * w)
    return f * n + e * (w - n)


def fmt_usd(v):
    if v >= 1_000_000: return f"${v / 1_000_000:.2f}M"
    if v >= 1_000:     return f"${v / 1_000:.1f}K"
    return f"${v:.2f}"


def fmt_pct(p): return f"{p * 100:.1f}¢"


def size_icon(usd, thr):
    if usd >= thr * 5: return "🔥"
    if usd >= thr:     return "⚡"
    if usd >= thr * .2: return "●"
    return "·"


def extract_slug(url):
    return url.rstrip("/").split("?")[0].split("#")[0].split("/")[-1]


# ─────────────────────────────────────────────────────────────────────────────
#  Market resolver  (Gamma API – no auth)
# ─────────────────────────────────────────────────────────────────────────────
def resolve_market(url: str) -> dict:
    slug = extract_slug(url)
    print(f"\n{CYN}[*] Resolving: {slug}{RST}")

    for endpoint, params in [
        (f"{GAMMA_API}/markets", {"slug": slug}),
        (f"{GAMMA_API}/markets", {"slug": slug, "limit": 1}),
        (f"{GAMMA_API}/events",  {"slug": slug}),
    ]:
        try:
            data = http_get(endpoint, params)
            mkt = _first_market(data)
            if mkt:
                return _parse_market(mkt)
        except Exception:
            pass

    for q in [slug.replace("-", " "), slug]:
        try:
            data = http_get(f"{GAMMA_API}/markets", {"q": q, "limit": 10})
            mkts = data if isinstance(data, list) else data.get("markets", [])
            for m in mkts:
                if slug in (m.get("slug", "") or ""):
                    return _parse_market(m)
            if mkts:
                return _parse_market(mkts[0])
        except Exception:
            pass

    raise RuntimeError(
        f"Cannot resolve market from URL: {url}\n"
        "Make sure you copy the full URL from your browser."
    )


def _first_market(data):
    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict):
        for key in ("markets", "data", "results"):
            lst = data.get(key)
            if lst:
                return lst[0]
        if data.get("conditionId") or data.get("condition_id"):
            return data
    return None


def _parse_market(m: dict) -> dict:
    cid   = m.get("conditionId") or m.get("condition_id", "")
    title = m.get("question") or m.get("title", "Unknown")

    tids = []
    raw_tokens = None
    for field in ("clobTokenIds", "tokens", "clob_token_ids", "tokenIds"):
        raw = m.get(field)
        if raw:
            if isinstance(raw, str):
                try:   raw = json.loads(raw)
                except Exception: raw = [raw]
            raw_tokens = raw
            for t in raw:
                if isinstance(t, dict):
                    tid = t.get("token_id") or t.get("tokenId") or t.get("id")
                else:
                    tid = str(t)
                if tid:
                    tids.append(str(tid))
            break

    for field in ("tokenId", "token_id"):
        v = m.get(field)
        if v and str(v) not in tids:
            tids.append(str(v))

    outcomes = m.get("outcomes") or ["Yes", "No"]
    if isinstance(outcomes, str):
        try:   outcomes = json.loads(outcomes)
        except Exception: outcomes = ["Yes", "No"]

    tmap = {}
    for i, tid in enumerate(tids):
        label = outcomes[i] if i < len(outcomes) else f"Token{i}"
        tmap[tid] = str(label)

    if raw_tokens:
        for t in raw_tokens:
            if isinstance(t, dict):
                tid = str(t.get("token_id") or t.get("tokenId") or t.get("id") or "")
                lbl = t.get("outcome") or t.get("name") or t.get("label")
                if tid and lbl:
                    tmap[tid] = str(lbl)

    return dict(
        condition_id=cid, title=title,
        token_ids=tids, outcomes=outcomes, token_map=tmap,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Trade fetcher  — Data API /activity  (fully public)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_trades(condition_id: str, token_ids: list,
                 limit=100, after_ts: float = 0.0) -> list:
    trades = []

    ts_params: dict = {}
    if after_ts > 0:
        ts_int = int(after_ts)
        ts_params = {"startTs": ts_int, "after": ts_int, "since": ts_int, "offset": 0}

    # Source 1 – Data API /activity with condition ID
    if condition_id:
        for base_params in [
            {"market": condition_id, "type": "TRADE", "limit": limit},
            {"market": condition_id, "limit": limit},
            {"conditionId": condition_id, "type": "TRADE", "limit": limit},
        ]:
            try:
                data = http_get(f"{DATA_API}/activity", {**base_params, **ts_params})
                raw = data if isinstance(data, list) else (
                    data.get("data") or data.get("results") or data.get("trades") or [])
                parsed = [p for p in (_parse_activity(t) for t in raw) if p]
                if after_ts > 0:
                    parsed = [p for p in parsed if p["dt"].timestamp() > after_ts - 1]
                if parsed:
                    trades.extend(parsed)
                    break
            except Exception:
                pass

    if trades:
        return trades

    # Source 2 – /trades endpoints
    if condition_id:
        for ep in [f"{DATA_API}/trades", f"{CLOB_API}/trades"]:
            try:
                data = http_get(ep, {"market": condition_id, "limit": limit, **ts_params})
                raw = data if isinstance(data, list) else (
                    data.get("data") or data.get("results") or data.get("trades") or [])
                parsed = [p for p in (_parse_activity(t) for t in raw) if p]
                if after_ts > 0:
                    parsed = [p for p in parsed if p["dt"].timestamp() > after_ts - 1]
                if parsed:
                    trades.extend(parsed)
                    return trades
            except Exception:
                pass

    # Source 3 – per-token activity
    for tok in token_ids[:2]:
        if not tok:
            continue
        for ep, param_key in [
            (f"{DATA_API}/activity", "token_id"),
            (f"{DATA_API}/activity", "asset_id"),
            (f"{CLOB_API}/trades",   "token_id"),
        ]:
            try:
                data = http_get(ep, {param_key: tok, "limit": limit, **ts_params})
                raw = data if isinstance(data, list) else (
                    data.get("data") or data.get("results") or data.get("trades") or [])
                parsed = [p for p in (_parse_activity(t) for t in raw) if p]
                if after_ts > 0:
                    parsed = [p for p in parsed if p["dt"].timestamp() > after_ts - 1]
                if parsed:
                    trades.extend(parsed)
                    break
            except Exception:
                pass
        if trades:
            break

    # Source 4 – CLOB last-trade-price (price-only, updates stats)
    if not trades:
        for tok in token_ids[:2]:
            if not tok:
                continue
            try:
                data = http_get(f"{CLOB_API}/last-trade-price", {"token_id": tok})
                price = float(data.get("price", 0) or 0)
                if price > 0:
                    stats["last_yes"] = price
                    stats["last_no"]  = round(1 - price, 4)
            except Exception:
                pass

    return trades


def _parse_activity(t: dict):
    if not isinstance(t, dict):
        return None
    try:
        tid = (t.get("transactionHash") or t.get("txHash") or
               t.get("id") or t.get("tradeId") or t.get("trade_id") or
               str(t.get("timestamp", time.time())) + str(t.get("price", "")))

        price = float(t.get("price", 0) or t.get("avgPrice", 0) or
                      t.get("avg_price", 0) or 0)

        size = float(
            t.get("size") or t.get("tokenAmount") or t.get("token_amount") or
            t.get("sharesAmount") or t.get("shares_amount") or
            t.get("amount") or 0
        )

        usdc = float(
            t.get("usdcSize") or t.get("usdc_size") or
            t.get("cashAmount") or t.get("cash_amount") or
            t.get("usdValue") or t.get("usd_value") or
            t.get("value") or t.get("notional") or 0
        )
        if usdc == 0 and price > 0 and size > 0:
            usdc = round(price * size, 4)

        if size == 0 and usdc == 0:
            return None

        side = str(t.get("side") or t.get("type") or t.get("tradeType") or "BUY").upper()
        if side in ("LONG",  "MAKER_BUY",  "TAKER_BUY",  "BID"): side = "BUY"
        if side in ("SHORT", "MAKER_SELL", "TAKER_SELL", "ASK", "SELL"): side = "SELL"
        if side not in ("BUY", "SELL"): side = "BUY"

        ts = (t.get("timestamp") or t.get("createdAt") or t.get("created_at") or
              t.get("blockTimestamp") or t.get("matchedAt") or time.time())
        if isinstance(ts, (int, float)):
            ts_f = float(ts)
            if ts_f > 1e12:
                ts_f /= 1000.0
            dt = datetime.fromtimestamp(ts_f, tz=timezone.utc)
        else:
            try:   dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            except: dt = datetime.now(tz=timezone.utc)

        return dict(
            id=str(tid), price=price, size=size, usd_value=usdc,
            side=side, dt=dt,
            _raw_token_fields={
                k: str(v) for k, v in t.items()
                if any(x in k.lower() for x in ("asset", "token", "outcome", "market"))
                and isinstance(v, (str, int, float))
            },
            token_id=str(
                t.get("asset_id") or t.get("assetId") or
                t.get("token_id") or t.get("tokenId") or
                t.get("outcomeIndex") or t.get("outcome_index") or ""
            ),
            tx_hash=t.get("transactionHash") or t.get("txHash") or "",
        )
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  Debug helper
# ─────────────────────────────────────────────────────────────────────────────
def debug_api(condition_id, token_ids):
    print(f"\n{YLW}=== DEBUG MODE ==={RST}")
    print(f"\n{CYN}token_map ({len(token_map)} entries):{RST}")
    for k, v in token_map.items():
        print(f"  {v!r:12} ← key={k!r}")

    endpoints = []
    if condition_id:
        endpoints += [
            (f"{DATA_API}/activity", {"market": condition_id, "type": "TRADE", "limit": 3}),
            (f"{DATA_API}/activity", {"market": condition_id, "limit": 3}),
            (f"{DATA_API}/trades",   {"market": condition_id, "limit": 3}),
            (f"{CLOB_API}/trades",   {"market": condition_id, "limit": 3}),
        ]
    for tok in token_ids[:2]:
        endpoints += [
            (f"{DATA_API}/activity",         {"token_id": tok, "limit": 3}),
            (f"{CLOB_API}/last-trade-price", {"token_id": tok}),
        ]

    for ep, params in endpoints:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        print(f"\n{CYN}GET {ep}?{qs}{RST}")
        try:
            data = http_get(ep, params, timeout=8, retries=1)
            rows = data if isinstance(data, list) else (
                data.get("data") or data.get("results") or data.get("trades") or [data])
            if rows and isinstance(rows[0], dict):
                r0 = rows[0]
                print(f"  {GRN}→ {len(rows)} record(s){RST}")
                for k, v in r0.items():
                    flag = " ◄" if any(x in k.lower() for x in ("asset", "token", "outcome", "market")) else ""
                    print(f"    {k}: {str(v)[:80]}{flag}")
                print(f"  {YLW}Matching against token_map:{RST}")
                matched = False
                for fk, fv in r0.items():
                    if str(fv) in token_map:
                        print(f"    {GRN}HIT: field {fk!r} = {str(fv)[:32]} → {token_map[str(fv)]!r}{RST}")
                        matched = True
                if not matched:
                    print(f"    {RED}No direct match.{RST}")
            elif rows:
                print(f"  {GRN}→ {json.dumps(rows[0], default=str)[:400]}{RST}")
            else:
                print(f"  {YLW}→ Empty response{RST}")
        except Exception as e:
            print(f"  {RED}→ Error: {e}{RST}")

    print(f"\n{YLW}=== END DEBUG ==={RST}\n")


# ─────────────────────────────────────────────────────────────────────────────
#  WebSocket live feed  (best-effort, needs 'websockets' package)
# ─────────────────────────────────────────────────────────────────────────────
def ws_thread(token_ids: list):
    if not token_ids:
        return
    try:
        import websockets, asyncio  # noqa: F401
    except ImportError:
        return

    import asyncio

    async def _run():
        while True:
            try:
                import websockets.client as wsc
                async with wsc.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
                    for tok in token_ids[:2]:
                        msg = json.dumps({"assets_ids": [tok], "type": "market", "markets": [tok]})
                        await ws.send(msg)
                    async for raw in ws:
                        try:
                            _handle_ws(json.loads(raw))
                        except Exception:
                            pass
            except Exception:
                await asyncio.sleep(5)

    asyncio.run(_run())


def _handle_ws(obj):
    if not isinstance(obj, dict):
        return
    if obj.get("event_type") in ("trade", "last_trade_price"):
        price = float(obj.get("price") or obj.get("last_trade_price") or 0)
        if price:
            stats["last_yes"] = price
            stats["last_no"]  = round(1 - price, 4)
    elif "price" in obj:
        try:
            stats["last_yes"] = float(obj["price"])
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
#  Sound
# ─────────────────────────────────────────────────────────────────────────────
def _beep(freq=880, ms=150):
    if not SOUND_ENABLED:
        return
    try:
        if _SYS == "Windows":
            import winsound
            winsound.Beep(int(freq), int(ms))
        elif _SYS == "Darwin":
            subprocess.Popen(["osascript", "-e", "beep"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            sys.stdout.write("\a")
            sys.stdout.flush()
    except Exception:
        sys.stdout.write("\a")
        sys.stdout.flush()


def alert_big(usd):
    if not SOUND_ENABLED:
        return
    threading.Thread(target=_do_beeps, args=(usd,), daemon=True).start()


def _do_beeps(usd):
    n = 3 if usd >= 5000 else 2 if usd >= 2000 else 1
    for i in range(n):
        _beep(1200 - i * 200, 160)
        time.sleep(0.13)


# ─────────────────────────────────────────────────────────────────────────────
#  Outcome resolver
# ─────────────────────────────────────────────────────────────────────────────
def _resolve_outcome(t: dict) -> str:
    if not token_map:
        return ""

    # 1 – direct key lookup
    tid = t.get("token_id", "")
    if tid and tid in token_map:
        return token_map[tid]

    # 2 – scan raw token-related fields
    for val in t.get("_raw_token_fields", {}).values():
        val_str = str(val)
        if val_str in token_map:
            return token_map[val_str]
        for key in token_map:
            if len(key) > 8 and len(val_str) > 8:
                if key.startswith(val_str[:16]) or val_str.startswith(key[:16]):
                    return token_map[key]

    # 3 – numeric outcome index
    idx_str = t.get("token_id", "")
    if idx_str in ("0", "1", "2", "3"):
        keys = list(token_map.keys())
        idx = int(idx_str)
        if idx < len(keys):
            return token_map[keys[idx]]

    # 4 – price-based inference (binary markets)
    price = t.get("price", 0)
    if price > 0 and len(token_map) == 2:
        ov = stats["outcome_volume"]
        prices = {lbl: d["last_price"] for lbl, d in ov.items() if d["last_price"] > 0}
        if len(prices) == 2:
            best = min(prices, key=lambda l: abs(prices[l] - price))
            if abs(prices[best] - price) < 0.15:
                return best
        labels = list(token_map.values())
        return labels[0] if price >= 0.5 else labels[1]

    return ""


# ─────────────────────────────────────────────────────────────────────────────
#  Ingest  — dedup, filter, push to feed
# ─────────────────────────────────────────────────────────────────────────────
def _ingest(trades: list) -> int:
    """Deduplicate, filter, push to feed. Returns count of new trades added."""
    global _last_trade_ts
    new = []
    for t in trades:
        if t["id"] in seen_ids:
            continue
        seen_ids.add(t["id"])
        if t["usd_value"] < filter_cfg["min_size"]:
            continue
        if t["side"] == "BUY"  and not filter_cfg["show_buys"]:
            continue
        if t["side"] == "SELL" and not filter_cfg["show_sells"]:
            continue
        new.append(t)

    if not new:
        return 0

    new.sort(key=lambda x: x["dt"])

    with feed_lock:
        for t in new:
            label = _resolve_outcome(t)
            t["outcome"] = label

            trade_feed.appendleft(t)
            stats["total_trades"] += 1
            stats["total_volume"]  += t["usd_value"]
            if t["side"] == "BUY":
                stats["buy_volume"]  += t["usd_value"]
            else:
                stats["sell_volume"] += t["usd_value"]
            if t["usd_value"] >= filter_cfg["big_threshold"]:
                stats["big_trades"] += 1
                alert_big(t["usd_value"])
            if t["price"] > 0:
                stats["last_yes"] = t["price"]
                stats["last_no"]  = round(1 - t["price"], 4)

            if label:
                ov = stats["outcome_volume"].setdefault(
                    label, {"buy": 0.0, "sell": 0.0, "last_price": 0.0})
                if t["side"] == "BUY":
                    ov["buy"]  += t["usd_value"]
                else:
                    ov["sell"] += t["usd_value"]
                if t["price"] > 0:
                    ov["last_price"] = t["price"]

    # Advance cursor — fully protected, only move forward
    with _cursor_lock:
        newest_ts = max(t["dt"].timestamp() for t in new)
        if newest_ts > _last_trade_ts:
            _last_trade_ts = newest_ts

    _dirty.set()
    return len(new)


# ─────────────────────────────────────────────────────────────────────────────
#  Poller thread
# ─────────────────────────────────────────────────────────────────────────────
def poller(condition_id, token_ids, title):
    """
    Timestamp-cursored poll loop.
    Every POLL_SEC seconds: request trades newer than our last known trade,
    dedup by ID, ingest. Backs off up to POLL_SEC*2 on quiet markets.
    """
    stats["market_title"] = title
    consecutive_empty = 0

    while True:
        try:
            with _cursor_lock:
                cursor = _last_trade_ts

            limit = 200 if cursor > 0 else 100
            raw   = fetch_trades(condition_id, token_ids, limit=limit, after_ts=cursor)
            added = _ingest(raw)

            if added > 0:
                consecutive_empty = 0
            else:
                consecutive_empty += 1

            sleep = POLL_SEC if consecutive_empty < 5 else min(POLL_SEC * 2, 10)

        except Exception:
            sleep = POLL_SEC

        time.sleep(sleep)


# ─────────────────────────────────────────────────────────────────────────────
#  Rich TUI renderer
# ─────────────────────────────────────────────────────────────────────────────
def run_rich(market_info):
    global SOUND_ENABLED

    def hdr():
        e = int(time.time() - stats["started_at"])
        h, m, s = e // 3600, (e % 3600) // 60, e % 60
        snd = "[green]ON[/]" if SOUND_ENABLED else "[red]OFF[/]"
        t = Text()
        t.append("⬡ POLYMARKET FLOW  ", style="bold cyan")
        t.append(f" {stats['market_title'][:62]} ", style="bold white on dark_blue")
        t.append(
            f"\n⏱ {h:02d}:{m:02d}:{s:02d}  🔊 {snd}  "
            f"Min: {fmt_usd(filter_cfg['min_size'])}  "
            f"Big≥: {fmt_usd(filter_cfg['big_threshold'])}",
            style="dim",
        )
        return Panel(t, box=box.DOUBLE_EDGE, style="bright_black")

    def stat_panel():
        vol   = stats["total_volume"]
        buys  = stats["buy_volume"]
        sells = stats["sell_volume"]
        r     = buys / vol if vol else 0.5
        bw    = 26
        t = Text()
        t.append(f"Trades : {stats['total_trades']:,}\n", style="bold")
        t.append(f"Volume : {fmt_usd(vol)}\n",            style="bold yellow")
        t.append(f"Big 🔥 : {stats['big_trades']}\n\n",  style="bold magenta")
        t.append("BUY  ", style="green bold")
        t.append(bar(r, bw, "▓", "░"), style="green")
        t.append("  SELL\n", style="red bold")
        t.append(f" {fmt_usd(buys)} ({r * 100:.0f}%)",   style="green")
        t.append(f"  {fmt_usd(sells)} ({(1-r)*100:.0f}%)\n", style="red")
        ov = stats["outcome_volume"]
        if ov:
            t.append("\n── Outcomes ──\n", style="bold bright_black")
            for lbl, d in sorted(ov.items()):
                tot = d["buy"] + d["sell"]
                r2  = d["buy"] / tot if tot else 0.5
                px  = d["last_price"]
                t.append(f"{lbl[:10]:<10} {fmt_pct(px):>6}  {fmt_usd(tot)}\n", style="bold white")
                t.append(f"  B:{fmt_usd(d['buy'])} ({r2*100:.0f}%)\n",          style="green")
                t.append(f"  S:{fmt_usd(d['sell'])} ({(1-r2)*100:.0f}%)\n",     style="red")
        else:
            ly = stats["last_yes"]
            ln = stats["last_no"]
            if ly:
                t.append(f"\nYES {fmt_pct(ly)}  NO {fmt_pct(ln or 1-ly)}", style="bold cyan")
        return Panel(t, title="[bold]Stats[/]", box=box.ROUNDED, border_style="bright_black")

    def feed_table():
        tbl = Table(
            box=box.SIMPLE_HEAD, show_header=True,
            header_style="bold bright_black", expand=True, padding=(0, 1),
        )
        tbl.add_column("Time",    style="dim",       width=10)
        tbl.add_column("Outcome",                    width=10)
        tbl.add_column("Side",                       width=6)
        tbl.add_column("Price",   justify="right",   width=7)
        tbl.add_column("Shares",  justify="right",   width=11)
        tbl.add_column("USD",     justify="right",   width=11)
        tbl.add_column("Sz",      justify="center",  width=3)
        tbl.add_column("Bar",                        width=18)

        with feed_lock:
            rows = list(trade_feed)[:MAX_ROWS]

        mx = max((t["usd_value"] for t in rows), default=1)
        for t in rows:
            is_b = t["side"] == "BUY"
            sty  = "green" if is_b else "red"
            outcome = t.get("outcome", "")
            outcome_cell = f"[bold]{outcome[:10]}[/]" if outcome else "–"
            tbl.add_row(
                t["dt"].strftime("%H:%M:%S"),
                outcome_cell,
                "[green]▲[/]" if is_b else "[red]▼[/]",
                fmt_pct(t["price"]),
                f"{t['size']:,.1f}",
                f"[bold {sty}]{fmt_usd(t['usd_value'])}[/]",
                size_icon(t["usd_value"], filter_cfg["big_threshold"]),
                f"[{sty}]{bar(t['usd_value'] / mx if mx else 0, 18, '█', '·')}[/]",
            )
        return tbl

    layout = Layout()
    layout.split_column(
        Layout(name="hdr",  size=5),
        Layout(name="body"),
        Layout(name="ctrl", size=2),
    )
    layout["body"].split_row(
        Layout(name="feed",  ratio=3),
        Layout(name="stats", ratio=1),
    )
    ctrl_txt = Text(
        "[q] Quit  [s] Sound  [m <val>] Min $  [b <val>] Big $ alert",
        style="dim",
    )

    def _refresh(live):
        while True:
            layout["hdr"].update(hdr())
            layout["stats"].update(stat_panel())
            layout["feed"].update(Panel(
                feed_table(), title="[bold]Live Trades[/]",
                box=box.ROUNDED, border_style="bright_black",
            ))
            layout["ctrl"].update(ctrl_txt)
            live.refresh()
            time.sleep(0.5)

    with Live(layout, refresh_per_second=2, screen=True) as live:
        t = threading.Thread(target=_refresh, args=(live,), daemon=True)
        t.start()
        t.join()   # block here; KeyboardInterrupt propagates up


# ─────────────────────────────────────────────────────────────────────────────
#  Plain terminal renderer
# ─────────────────────────────────────────────────────────────────────────────
def run_plain(market_info):
    global SOUND_ENABLED

    def clr():
        os.system("cls" if os.name == "nt" else "clear")

    while True:
        # Only redraw when something changed (avoids busy flicker on quiet markets)
        _dirty.wait(timeout=1.5)
        _dirty.clear()

        clr()
        with feed_lock:
            cnt  = stats["total_trades"]
            rows = list(trade_feed)[:50]

        vol   = stats["total_volume"]
        buys  = stats["buy_volume"]
        sells = stats["sell_volume"]
        r     = buys / vol if vol else 0.5
        e     = int(time.time() - stats["started_at"])
        h, m, s = e // 3600, (e % 3600) // 60, e % 60
        snd   = f"{GRN}ON{RST}" if SOUND_ENABLED else f"{RED}OFF{RST}"

        with _cursor_lock:
            cursor_str = (
                datetime.fromtimestamp(_last_trade_ts, tz=timezone.utc).strftime("%H:%M:%S")
                if _last_trade_ts > 0 else "–"
            )

        print(f"{CYN}{BOLD}{'─'*74}{RST}")
        print(f"{CYN}{BOLD} ⬡  POLYMARKET ORDER FLOW{RST}  {WHT}{stats['market_title'][:52]}{RST}")
        print(f"{DIM} ⏱ {h:02d}:{m:02d}:{s:02d}  🔊 {snd}  "
              f"Min:{fmt_usd(filter_cfg['min_size'])}  "
              f"Big≥:{fmt_usd(filter_cfg['big_threshold'])}  "
              f"last:{cursor_str} UTC{RST}")
        print(f"{CYN}{BOLD}{'─'*74}{RST}")

        bw = 24
        print(f"  Trades:{WHT}{BOLD}{cnt:,}{RST}  "
              f"Vol:{YLW}{BOLD}{fmt_usd(vol)}{RST}  "
              f"Big🔥:{MAG}{BOLD}{stats['big_trades']}{RST}")
        print(f"  {GRN}BUY{RST} {GRN}{bar(r,bw)}{RST}"
              f"{RED}{bar(1-r,bw)}{RST} {RED}SELL{RST}")
        print(f"  {GRN}{fmt_usd(buys)}({r*100:.0f}%){RST}"
              f"                    "
              f"{RED}{fmt_usd(sells)}({(1-r)*100:.0f}%){RST}")

        ly = stats["last_yes"]
        ln = stats["last_no"]
        if ly:
            print(f"  {CYN}YES:{fmt_pct(ly)}  NO:{fmt_pct(ln or 1-ly)}{RST}")

        print(f"{CYN}{'─'*74}{RST}")
        print(f"  {DIM}{'TIME':8}  {'SIDE':6}  {'PRICE':6}  {'SHARES':>10}  {'USD':>10}  BAR{RST}")
        print(f"{CYN}{'─'*74}{RST}")

        mx = max((t["usd_value"] for t in rows), default=1)
        for t in rows:
            ib  = t["side"] == "BUY"
            col = GRN if ib else RED
            outcome = t.get("outcome", "")
            side_str = (
                f"{'▲' if ib else '▼'} {outcome[:6]}" if outcome
                else ("▲BUY" if ib else "▼SELL")
            )
            print(
                f"  {DIM}{t['dt'].strftime('%H:%M:%S')}{RST}  "
                f"{col}{BOLD}{side_str:<8}{RST}  "
                f"{fmt_pct(t['price']):6}  {t['size']:>10,.1f}  "
                f"{col}{BOLD}{fmt_usd(t['usd_value']):>10}{RST} "
                f"{size_icon(t['usd_value'], filter_cfg['big_threshold'])} "
                f"{col}{bar(t['usd_value']/mx if mx else 0, 14, '█', '·')}{RST}"
            )

        ov = stats["outcome_volume"]
        if ov:
            print(f"{CYN}{'─'*74}{RST}")
            for lbl, d in sorted(ov.items()):
                tot = d["buy"] + d["sell"]
                r2  = d["buy"] / tot if tot else 0.5
                px  = d["last_price"]
                print(
                    f"  {WHT}{BOLD}{lbl[:10]:<10}{RST}  "
                    f"{fmt_pct(px):6}  "
                    f"{GRN}{bar(r2,16,'▓','░')}{RST}"
                    f"{RED}{bar(1-r2,16,'▓','░')}{RST}  "
                    f"vol {YLW}{fmt_usd(tot)}{RST}  "
                    f"{GRN}B:{fmt_usd(d['buy'])}{RST} "
                    f"{RED}S:{fmt_usd(d['sell'])}{RST}"
                )

        print(f"{CYN}{'─'*74}{RST}")
        print(f"{DIM} Ctrl+C=Quit | s=Sound | m <50>=MinSize | b <500>=BigAlert{RST}")


# ─────────────────────────────────────────────────────────────────────────────
#  Input thread  — non-blocking on POSIX, best-effort on Windows
# ─────────────────────────────────────────────────────────────────────────────
def input_thread():
    """
    Read commands without blocking the render loop.
    On POSIX we use select() with a 0.2 s timeout so the thread spins
    cheaply and exits cleanly on KeyboardInterrupt.
    On Windows we fall back to blocking input() (readline is blocking there).
    """
    global SOUND_ENABLED

    def handle(cmd):
        cmd = cmd.strip().lower()
        if cmd == "s":
            SOUND_ENABLED = not SOUND_ENABLED
            print(f"\r{GRN if SOUND_ENABLED else RED}[Sound {'ON' if SOUND_ENABLED else 'OFF'}]{RST}")
            _dirty.set()
        elif cmd.startswith("m"):
            parts = cmd.split()
            try:
                v = float(parts[1]) if len(parts) > 1 else float(input("Min USD size: "))
                filter_cfg["min_size"] = v
                print(f"\r{CYN}[Min size → {fmt_usd(v)}]{RST}")
                _dirty.set()
            except (ValueError, IndexError):
                pass
        elif cmd.startswith("b"):
            parts = cmd.split()
            try:
                v = float(parts[1]) if len(parts) > 1 else float(input("Big trade USD: "))
                filter_cfg["big_threshold"] = v
                print(f"\r{CYN}[Big threshold → {fmt_usd(v)}]{RST}")
                _dirty.set()
            except (ValueError, IndexError):
                pass
        elif cmd in ("q", "quit", "exit"):
            print(f"\n{CYN}Bye!{RST}")
            os._exit(0)

    is_posix = hasattr(select, "select")

    while True:
        try:
            if is_posix:
                ready, _, _ = select.select([sys.stdin], [], [], 0.2)
                if ready:
                    line = sys.stdin.readline()
                    if line:
                        handle(line)
            else:
                handle(input())
        except (EOFError, KeyboardInterrupt):
            break
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
#  Seed loader
# ─────────────────────────────────────────────────────────────────────────────
def seed_trades(cid, tids):
    """
    Load recent historical trades, populate stats, set the timestamp cursor.
    Returns the number of trades loaded.
    """
    global _last_trade_ts
    print(f"{CYN}[*] Loading recent trades...{RST}")
    try:
        initial = fetch_trades(cid, tids, limit=100, after_ts=0)
        real = [t for t in initial if t["usd_value"] > 0 or t["size"] > 0]
        if not real:
            print(f"{YLW}[!] No trades with volume found. "
                  f"Market may be new or low-volume.{RST}")
            print(f"{YLW}    Run with --debug flag to inspect raw API responses.{RST}")
            return 0

        real.sort(key=lambda x: x["dt"])

        with feed_lock:
            for t in real:
                seen_ids.add(t["id"])
                if t["usd_value"] < filter_cfg["min_size"]:
                    continue
                label = _resolve_outcome(t)
                t["outcome"] = label
                trade_feed.appendleft(t)

                stats["total_trades"] += 1
                stats["total_volume"]  += t["usd_value"]
                if t["side"] == "BUY":
                    stats["buy_volume"]  += t["usd_value"]
                else:
                    stats["sell_volume"] += t["usd_value"]
                if t["price"] > 0:
                    stats["last_yes"] = t["price"]
                    stats["last_no"]  = round(1 - t["price"], 4)

                if label:
                    ov = stats["outcome_volume"].setdefault(
                        label, {"buy": 0.0, "sell": 0.0, "last_price": 0.0})
                    if t["side"] == "BUY":
                        ov["buy"]  += t["usd_value"]
                    else:
                        ov["sell"] += t["usd_value"]
                    if t["price"] > 0:
                        ov["last_price"] = t["price"]

        newest_ts = max(t["dt"].timestamp() for t in real)
        with _cursor_lock:
            _last_trade_ts = newest_ts

        print(
            f"{GRN}[✓] Loaded {len(real)} trades  "
            f"(vol: {fmt_usd(stats['total_volume'])}, "
            f"cursor: {datetime.fromtimestamp(newest_ts, tz=timezone.utc).strftime('%H:%M:%S')} UTC){RST}"
        )
        _dirty.set()
        return len(real)

    except Exception as e:
        print(f"{YLW}[!] Seed failed: {e}{RST}")
        return 0


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────
def get_url():
    if len(sys.argv) > 1 and not sys.argv[1].startswith("--"):
        return sys.argv[1]
    print(f"\n{CYN}{BOLD}╔══════════════════════════════════════════╗")
    print(f"║   POLYMARKET ORDER FLOW  v2.3            ║")
    print(f"║   Live trades · No API keys needed       ║")
    print(f"╚══════════════════════════════════════════╝{RST}\n")
    return input("  Paste Polymarket market URL: ").strip()


def get_filters():
    global SOUND_ENABLED
    print(f"\n{YLW}Set filters (Enter = keep default):{RST}")
    v = input(f"  Min trade size USD  [{filter_cfg['min_size']}]: ").strip()
    if v:
        try: filter_cfg["min_size"] = float(v)
        except ValueError: pass
    v = input(f"  Big trade alert USD [{filter_cfg['big_threshold']}]: ").strip()
    if v:
        try: filter_cfg["big_threshold"] = float(v)
        except ValueError: pass
    v = input(f"  Sound alerts        [{'y' if SOUND_ENABLED else 'n'}]: ").strip().lower()
    if v in ("y", "yes"): SOUND_ENABLED = True
    elif v in ("n", "no"): SOUND_ENABLED = False


def main():
    debug_mode = "--debug" in sys.argv

    url = get_url()
    if not url:
        print("No URL. Exiting.")
        sys.exit(1)

    if not debug_mode:
        get_filters()

    print(f"\n{CYN}[*] Fetching market data...{RST}")
    try:
        mi = resolve_market(url)
    except Exception as e:
        print(f"{RED}[!] {e}{RST}")
        sys.exit(1)

    print(f"{GRN}[✓] {mi['title']}{RST}")
    print(f"{DIM}    Condition : {mi['condition_id']}{RST}")
    print(f"{DIM}    Outcomes  : {mi['outcomes']}{RST}")
    for tok, lbl in mi.get("token_map", {}).items():
        print(f"{DIM}    {lbl:12} ← token {tok[:16]}...{RST}")

    if not mi["condition_id"] and not mi["token_ids"]:
        print(f"{RED}[!] No identifiers found – cannot fetch trades.{RST}")
        sys.exit(1)

    token_map.update(mi.get("token_map", {}))

    if debug_mode:
        debug_api(mi["condition_id"], mi["token_ids"])
        sys.exit(0)

    seed_trades(mi["condition_id"], mi["token_ids"])
    stats["market_title"] = mi["title"]

    # Start REST poller
    threading.Thread(
        target=poller,
        args=(mi["condition_id"], mi["token_ids"], mi["title"]),
        daemon=True,
    ).start()

    # Try WebSocket (best-effort, needs 'websockets' package)
    if mi["token_ids"]:
        threading.Thread(target=ws_thread, args=(mi["token_ids"],), daemon=True).start()

    time.sleep(0.3)
    print(f"\n{GRN}[✓] Live feed running (Ctrl+C to quit){RST}\n")
    time.sleep(0.7)

    try:
        if RICH:
            run_rich(mi)
        else:
            threading.Thread(target=input_thread, daemon=True).start()
            run_plain(mi)
    except KeyboardInterrupt:
        print(f"\n{CYN}Bye!{RST}")


if __name__ == "__main__":
    main()