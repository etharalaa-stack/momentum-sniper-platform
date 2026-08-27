"""Comprehensive Diagnostic Tool for Sniper Platform"""
from __future__ import annotations

import asyncio
import sqlite3
import json
from pathlib import Path
import aiohttp
from sniper.config import DEFAULT

async def run_diagnostics():
    print("=" * 60)
    print("🔍 SNIPER PLATFORM DIAGNOSTIC REPORT")
    print("=" * 60)

    # 1. Check Configuration & Dry Run Status
    print(f"\n[1] Configuration Check:")
    print(f"    - Exchange: {DEFAULT.exchange}")
    print(f"    - Symbols: {DEFAULT.symbols}")
    print(f"    - Dry Run Mode: {DEFAULT.dry_run}")
    print(f"    - Database Path: {DEFAULT.db_path}")

    # 2. Check SQLite Database & Trades Table
    print(f"\n[2] Database Inspection ({DEFAULT.db_path}):")
    db_file = Path(DEFAULT.db_path)
    if not db_file.exists():
        print("    [-] WARNING: Database file does not exist yet! Engine hasn't written anything to DB.")
    else:
        try:
            conn = sqlite3.connect(DEFAULT.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"    [+] Tables found in DB: {tables}")

            if "trades" in tables:
                cursor.execute("SELECT COUNT(*) FROM trades;")
                count = cursor.fetchone()[0]
                print(f"    [+] Total trades recorded in DB: {count}")
                if count > 0:
                    cursor.execute("SELECT id, symbol, entry_price, pnl_usdt, is_win FROM trades ORDER BY id DESC LIMIT 5;")
                    rows = cursor.fetchall()
                    print(f"    [+] Last recorded trades:")
                    for r in rows:
                        print(f"        -> ID: {r[0]} | Symbol: {r[1]} | Entry: {r[2]} | PnL: {r[3]} | Win: {r[4]}")
                else:
                    print("    [-] Database is empty (0 trades found in table).")
            else:
                print("    [-] WARNING: 'trades' table is missing from database.")
            conn.close()
        except Exception as e:
            print(f"    [-] Database Error: {e}")

    # 3. Check Binance Connectivity & Orderbooks
    print(f"\n[3] Binance Market Connectivity Check:")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{DEFAULT.rest_base}/api/v3/ping", timeout=5) as resp:
                if resp.status == 200:
                    print("    [+] Binance REST API Ping: SUCCESS")
                else:
                    print(f"    [-] Binance REST API Ping failed with status {resp.status}")
        except Exception as e:
            print(f"    [-] Binance REST API Connection Error: {e}")

        for symbol in DEFAULT.symbols:
            try:
                url = f"{DEFAULT.rest_base}/api/v3/depth?symbol={symbol}&limit=5"
                async with session.get(url, timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        bids = data.get("bids", [])
                        asks = data.get("asks", [])
                        top_bid = bids[0][0] if bids else "N/A"
                        top_ask = asks[0][0] if asks else "N/A"
                        print(f"    [+] Symbol {symbol}: OK | Best Bid: {top_bid} | Best Ask: {top_ask}")
                    else:
                        print(f"    [-] Symbol {symbol}: Failed (HTTP {resp.status})")
            except Exception as e:
                print(f"    [-] Symbol {symbol}: Exception -> {e}")

    # 4. Check Local Backend API & Ports
    print(f"\n[4] Local Backend API & Ports Check:")
    ports_path = Path(DEFAULT.ports_file)
    if ports_path.exists():
        try:
            with open(ports_path, "r") as f:
                ports_data = json.load(f)
            print(f"    [+] Found ports.json: {ports_data}")
            
            # Fix: Handle nested structure in ports.json
            api_info = ports_data.get("api", {})
            api_port = api_info.get("port")
            api_url = api_info.get("url")
            
            if api_port:
                async with aiohttp.ClientSession() as session:
                    url = f"{api_url}/api/analytics"
                    async with session.get(url, timeout=3) as resp:
                        status = resp.status
                        print(f"    [+] Local Backend API Status: {status} (OK)")
                        if status == 200:
                            body = await resp.json()
                            print(f"    [+] Analytics Data: {body}")
        except Exception as e:
            print(f"    [-] Error communicating with local API: {e}")
        except Exception as e:
            print(f"    [-] Error communicating with local API: {e}")
    else:
        print("    [-] ports.json not found. Make sure backend platform is running.")

    print("\n" + "=" * 60)
    print("DIAGNOSTIC COMPLETED")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(run_diagnostics())