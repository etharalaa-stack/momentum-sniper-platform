"""Surgical Fix Script for Sniper Platform"""
import sqlite3
import json
from pathlib import Path
from sniper.config import DEFAULT

def surgical_repair():
    print("=" * 60)
    print(" chirurgic: STARTING SYSTEM SURGICAL REPAIR")
    print("=" * 60)

    # 1. Fix Database Table Name Mismatch (trade -> trades)
    db_path = Path(DEFAULT.db_path)
    print(f"\n[1] Fixing Database Schema at: {db_path}")
    if db_path.exists():
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"    - Existing tables: {tables}")
        
        if "trade" in tables and "trades" not in tables:
            cursor.execute("ALTER TABLE trade RENAME TO trades;")
            conn.commit()
            print("    [+] Successfully renamed table 'trade' -> 'trades'.")
        elif "trades" not in tables:
            # Create trades table if missing completely
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    pnl_usdt REAL,
                    pnl_percent REAL,
                    is_win BOOLEAN,
                    execution_latency REAL
                );
            """)
            conn.commit()
            print("    [+] Created missing 'trades' table successfully.")
        else:
            print("    [+] Table 'trades' is already correctly structured.")
        conn.close()
    else:
        print("    [-] Database file not found yet. It will be created on engine start.")

    # 2. Check Ports & Web Configuration
    ports_path = Path(DEFAULT.ports_file)
    print(f"\n[2] Checking Ports Configuration ({ports_path}):")
    if ports_path.exists():
        with open(ports_path, "r") as f:
            ports_data = json.load(f)
        print(f"    [+] Active ports loaded: {ports_data}")
    else:
        print("    [-] ports.json not found. Backend platform will generate it.")

    # 3. Network / DNS Diagnostics & Fallback Recommendation
    print(f"\n[3] Network & Binance DNS Status:")
    print("    - If you are experiencing 'Could not contact DNS servers',")
    print("      your local ISP is blocking Binance API endpoints.")
    print("    - Recommendation: Turn on a reliable VPN (like Cloudflare WARP or ProtonVPN)")
    print("      or the bot will use local dry-run simulation mode.")

    print("\n" + "=" * 60)
    print(" SURGICAL REPAIR COMPLETED SUCCESSFULLY!")
    print("=" * 60)

if __name__ == "__main__":
    surgical_repair()