from __future__ import annotations

import math
import re
import statistics
import time
from dataclasses import asdict, dataclass
from typing import Any

from . import market_service

QUERY_MAX_PRICE_SAMPLES = 100_000


def _normalized_price(price: int | float, currency: int) -> int | float:
    return price * 100_000_000 if currency == 1 else price


@dataclass
class ItemStats:
    item: str
    days: int
    trades: int
    median: int
    average: int
    low: int
    high: int
    latest: int
    latest_at: str | None
    sample_limited: bool



def search_items(query: str, limit: int = 10) -> list[str]:
    q = query.strip().lower()
    if not q:
        return []
    items = market_service.get_items()
    exact_prefix = [i["display_name"] for i in items if i["display_name"].lower().startswith(q)]
    contains = [
        i["display_name"] for i in items
        if q in i["display_name"].lower() and not i["display_name"].lower().startswith(q)
    ]
    return (exact_prefix + contains)[: max(1, min(limit, 25))]


def resolve_item(query: str) -> str | None:
    q = query.strip().lower()
    if not q:
        return None
    names = search_items(q, 25)
    exact = next((name for name in names if name.lower() == q), None)
    return exact or (names[0] if names else None)


def get_item_stats(item_query: str, days: int = 30) -> dict[str, Any] | None:
    item = resolve_item(item_query) or item_query.strip()
    if not item:
        return None
    days = max(1, min(days, 3650))
    cutoff = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - days * 86400))
    rows = market_service.get_conn().execute(
        "SELECT timestamp, price, currency FROM trades "
        "WHERE LOWER(item_name)=LOWER(?) AND timestamp>=? AND price>0 "
        "ORDER BY timestamp DESC LIMIT ?",
        (item, cutoff, QUERY_MAX_PRICE_SAMPLES + 1),
    ).fetchall()
    if not rows:
        return None
    sample_limited = len(rows) > QUERY_MAX_PRICE_SAMPLES
    if sample_limited:
        rows = rows[:QUERY_MAX_PRICE_SAMPLES]
    prices = [float(_normalized_price(row[1], row[2])) for row in rows]
    prices_sorted = sorted(prices)
    stats = ItemStats(
        item=item,
        days=days,
        trades=len(prices),
        median=round(statistics.median(prices)),
        average=round(statistics.fmean(prices)),
        low=round(prices_sorted[max(0, math.floor((len(prices_sorted) - 1) * 0.10))]),
        high=round(prices_sorted[min(len(prices_sorted) - 1, math.ceil((len(prices_sorted) - 1) * 0.90))]),
        latest=round(prices[0]),
        latest_at=rows[0][0],
        sample_limited=sample_limited,
    )
    return asdict(stats)


def get_recent_item_trades(item_query: str, limit: int = 10) -> list[dict[str, Any]]:
    item = resolve_item(item_query) or item_query.strip()
    limit = max(1, min(limit, 25))
    rows = market_service.get_conn().execute(
        "SELECT id,timestamp,item_name,quantity,price,currency,seller,buyer "
        "FROM trades WHERE LOWER(item_name)=LOWER(?) "
        "ORDER BY id DESC LIMIT ?",
        (item, limit),
    ).fetchall()
    return [
        {
            "id": row[0], "timestamp": row[1], "item": row[2], "quantity": row[3],
            "price": round(_normalized_price(row[4], row[5])), "currency": row[5],
            "seller": row[6], "buyer": row[7],
        }
        for row in rows
    ]


def get_player_trades(username: str, limit: int = 10) -> list[dict[str, Any]]:
    name = market_service.normalize_username(username)
    limit = max(1, min(limit, 25))
    rows = market_service.get_conn().execute(
        "SELECT id,timestamp,item_name,quantity,price,currency,seller,buyer "
        "FROM trades WHERE LOWER(seller)=? OR LOWER(buyer)=? "
        "ORDER BY id DESC LIMIT ?",
        (name, name, limit),
    ).fetchall()
    return [
        {
            "id": row[0], "timestamp": row[1], "item": row[2], "quantity": row[3],
            "price": round(_normalized_price(row[4], row[5])), "currency": row[5],
            "seller": row[6], "buyer": row[7],
            "role": "seller" if (row[6] or "").lower() == name else "buyer",
        }
        for row in rows
    ]


def get_hot_items(hours: int = 24, limit: int = 10) -> list[dict[str, Any]]:
    hours = max(1, min(hours, 720))
    limit = max(1, min(limit, 25))
    cutoff = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - hours * 3600))
    rows = market_service.get_conn().execute(
        "SELECT item_name, COUNT(*) AS cnt FROM trades "
        "WHERE timestamp>=? GROUP BY item_name ORDER BY cnt DESC LIMIT ?",
        (cutoff, limit),
    ).fetchall()
    return [{"item": row[0], "trades": row[1]} for row in rows]


def get_status() -> dict[str, Any]:
    conn = market_service.get_conn()
    total_trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    backfilled = conn.execute("SELECT COUNT(*) FROM backfill_status").fetchone()[0]
    latest = conn.execute("SELECT MAX(timestamp) FROM trades").fetchone()[0]
    return {
        "total_trades": total_trades,
        "backfilled_items": backfilled,
        "latest_trade": latest,
        "workers": market_service.WORKERS,
        "polling_interval": market_service.POLLING_INTERVAL,
        "db_path": market_service.DB_PATH,
    }


def ask_database(question: str) -> str:
    """Deterministic natural-language router; no arbitrary SQL or hallucinated data."""
    raw = question.strip()
    lower = raw.lower()
    if not raw:
        return "Ask about an item price, recent item trades, a player, hot items, or status."

    if any(word in lower for word in ("status", "database size", "scraper")):
        status = get_status()
        return (
            f"Database has **{status['total_trades']:,}** trades across "
            f"**{status['backfilled_items']:,}** backfilled items. "
            f"Latest stored trade: `{status['latest_trade'] or 'none'}`."
        )

    hot_match = re.search(r"(?:hot|popular|most traded)(?: items?)?(?:.*?(\d+)\s*h)?", lower)
    if hot_match:
        hours = int(hot_match.group(1) or 24)
        items = get_hot_items(hours, 10)
        if not items:
            return f"No trades were found in the last {hours} hours."
        return f"Most traded in the last {hours}h:\n" + "\n".join(
            f"• **{entry['item']}** — {entry['trades']:,} trades" for entry in items
        )

    player_match = re.search(r"(?:player|user|trades? (?:for|by))\s+(.+)$", raw, re.IGNORECASE)
    if player_match:
        player = player_match.group(1).strip(" ?")
        trades = get_player_trades(player, 10)
        if not trades:
            return f"No trades found for **{player}**."
        return f"Recent trades for **{player}**:\n" + "\n".join(
            f"• `{t['timestamp']}` — {t['role']} {t['quantity']}x **{t['item']}** at {t['price']:,}"
            for t in trades
        )

    item_query = re.sub(
        r"^(?:what(?:'s| is)?|show|tell me|price(?: of)?|recent trades?(?: for)?|how much is)\s+",
        "",
        raw,
        flags=re.IGNORECASE,
    ).strip(" ?")
    item = resolve_item(item_query)
    if item:
        stats = get_item_stats(item, 30)
        if not stats:
            return f"I found **{item}**, but no trades in the last 30 days."
        return (
            f"**{stats['item']}** over {stats['days']} days: median **{stats['median']:,}**, "
            f"average **{stats['average']:,}**, typical range **{stats['low']:,}–{stats['high']:,}**, "
            f"latest **{stats['latest']:,}** from {stats['trades']:,}"
            f"{'+' if stats.get('sample_limited') else ''} sampled trades."
        )

    return (
        "I could not resolve that question. Try `/price <item>`, `/recent <item>`, "
        "`/player <name>`, `/hotitems`, or ask `price of <item>`."
    )
