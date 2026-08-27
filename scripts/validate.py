#!/usr/bin/env python3
"""Pre-flight validation — env, DB, WebSocket connectivity."""
from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

REQUIRED_ENV = ("SNIPER_DRY_RUN", "SNIPER_DASHBOARD_KEY", "SNIPER_JWT_SECRET")
WARN_IF_DEFAULT = ("SNIPER_JWT_SECRET", "SNIPER_DASHBOARD_KEY")


def check_env() -> list[str]:
    errs: list[str] = []
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())
    for key in REQUIRED_ENV:
        if not os.getenv(key):
            errs.append(f"missing env: {key}")
    for key in WARN_IF_DEFAULT:
        val = os.getenv(key, "")
        if "change-me" in val or "dev-dashboard" in val:
            print(f"  [warn] {key} uses dev default — rotate for production")
    dry = os.getenv("SNIPER_DRY_RUN", "1")
    print(f"  [ok] SNIPER_DRY_RUN={dry}")
    return errs


def check_db() -> list[str]:
    db = Path(os.getenv("SNIPER_DB", str(ROOT / "sniper.db")))
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            entry_time DATETIME NOT NULL,
            exit_time DATETIME,
            entry_price REAL NOT NULL,
            exit_price REAL,
            pnl_usdt REAL,
            pnl_percent REAL,
            is_win BOOLEAN,
            execution_latency REAL NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS market_regimes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at DATETIME NOT NULL,
            symbol TEXT,
            hour_utc INTEGER NOT NULL,
            regime TEXT NOT NULL,
            volatility REAL,
            avg_spread REAL,
            trade_count INTEGER,
            win_rate REAL,
            performance_score REAL
        );
    """)
    conn.commit()
    conn.close()
    print(f"  [ok] DB schema ready: {db}")
    return []


async def check_ws() -> list[str]:
    errs: list[str] = []
    try:
        import aiohttp
    except ImportError:
        return ["aiohttp not installed"]
    urls = [
        "wss://stream.binance.com:9443/ws/btcusdt@bookTicker",
        "wss://api.gateio.ws/ws/v4/",
    ]
    timeout = aiohttp.ClientTimeout(total=10, connect=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for url in urls:
            try:
                async with session.ws_connect(url, heartbeat=20, compress=0) as ws:
                    if "gateio" in url:
                        import time
                        await ws.send_json({
                            "time": int(time.time()),
                            "channel": "spot.book_ticker",
                            "event": "subscribe",
                            "payload": ["BTC_USDT"],
                        })
                    msg = await asyncio.wait_for(ws.receive(), timeout=8)
                    if msg.type.name in ("TEXT", "BINARY"):
                        print(f"  [ok] WS connected: {url[:50]}...")
                    else:
                        errs.append(f"WS no data: {url}")
            except Exception as exc:
                errs.append(f"WS fail {url}: {exc}")
    return errs


def check_imports() -> list[str]:
    errs: list[str] = []
    try:
        import arb_engine  # noqa: F401
        print("  [ok] arb_engine imports")
    except Exception as exc:
        errs.append(f"arb_engine import: {exc}")
    return errs


async def main() -> int:
    print("==> Validating Momentum Sniper deployment")
    all_errs: list[str] = []
    all_errs += check_env()
    all_errs += check_db()
    all_errs += check_imports()
    all_errs += await check_ws()
    if all_errs:
        print("\n==> FAILED:")
        for e in all_errs:
            print(f"  - {e}")
        return 1
    print("\n==> All checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
