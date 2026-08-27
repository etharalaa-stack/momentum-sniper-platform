#!/usr/bin/env python3
"""Standalone FastAPI ledger server for arb_engine + Next.js dashboard."""
from __future__ import annotations

import os

import uvicorn

from sniper.backend import Ledger, create_api
from sniper.config import DEFAULT, Config


def main() -> None:
    host = os.getenv("SNIPER_BIND_HOST", DEFAULT.bind_host)
    port = int(os.getenv("SNIPER_API_PORT", "8787"))
    cfg = Config(
        bind_host=host,
        db_path=os.getenv("SNIPER_DB", DEFAULT.db_path),
        jwt_secret=os.getenv("SNIPER_JWT_SECRET", DEFAULT.jwt_secret),
        dashboard_key=os.getenv("SNIPER_DASHBOARD_KEY", DEFAULT.dashboard_key),
    )
    ledger = Ledger(cfg)
    platform = create_api(cfg, ledger, live_fn=lambda: {"engine": "momentum", "dry_run": cfg.dry_run})
    uvicorn.run(platform.app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
