"""Runtime configuration — env overrides, no hardcoded ports."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True, slots=True)
class Config:
    exchange: str = os.getenv("SNIPER_EXCHANGE", "binance")
    symbols: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            s.strip() for s in os.getenv("SNIPER_SYMBOLS", "BTCUSDT,ETHUSDT,XRPUSDT,BNBUSDT,SOLUSDT,TAOUSDT").split(",") if s.strip()
        )
    )
    ws_base: str = os.getenv("SNIPER_WS_BASE", "wss://stream.binance.com:9443/ws")
    rest_base: str = os.getenv("SNIPER_REST_BASE", "https://api.binance.com")

    depth_levels: int = 20
    imbalance_threshold: float = 0.20  # تم تخفيفه من 0.35 لتسريع كشف الاختلالات
    wall_qty_mult: float = 2.0         # تم تخفيفه من 4.0 (أصبح الضعف بدلاً من 4 أضعاف)
    wall_consume_ms: int = 150         # تم زيادته من 80 إلى 150 مللي ثانية لإعطاء وسع زمني
    max_spread_bps: float = 4.0        # تم رفعه من 2.0 إلى 4.0 لتجنب رفض الصفقات الضيقة

    capital_allocation_pct: float = float(os.getenv("SNIPER_ALLOC_PCT", "0.10"))
    virtual_balance_usdt: float = float(os.getenv("SNIPER_VIRTUAL_BALANCE", "10000"))
    max_open_positions: int = int(os.getenv("SNIPER_MAX_OPEN", "5"))

    trail_hard_trigger_pct: float = 0.20
    trail_hard_floor_pct: float = 0.15
    trail_wide_trigger_pct: float = 0.50
    trail_wide_floor_pct: float = 0.40

    bind_host: str = os.getenv("SNIPER_BIND_HOST", "127.0.0.1")
    db_path: str = os.getenv("SNIPER_DB", "D:/Algorithmic Trading/sniper.db")
    ports_file: str = os.getenv("SNIPER_PORTS_FILE", str(ROOT / "ports.json"))
    jwt_secret: str = os.getenv("SNIPER_JWT_SECRET", "change-me-in-production")
    dashboard_key: str = os.getenv("SNIPER_DASHBOARD_KEY", "dev-dashboard-key")
    master_key: str = os.getenv("SNIPER_MASTER_KEY", "")
    api_key_enc: str = os.getenv("BINANCE_API_KEY_ENC", "")
    api_secret_enc: str = os.getenv("BINANCE_API_SECRET_ENC", "")
    api_key_plain: str = os.getenv("BINANCE_API_KEY", "")
    api_secret_plain: str = os.getenv("BINANCE_API_SECRET", "")

    dry_run: bool = os.getenv("SNIPER_DRY_RUN", "1") == "1"
    uvloop: bool = os.getenv("SNIPER_UVLOOP", "1") == "1"
    ml_epsilon: float = float(os.getenv("SNIPER_ML_EPSILON", "0.15"))
    rate_limit: str = os.getenv("SNIPER_RATE_LIMIT", "60/minute")

    extra: dict = field(default_factory=dict)


DEFAULT = Config()