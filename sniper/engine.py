"""Orchestrator — L2 feed, execution, ledger hooks, ML param sync."""
from __future__ import annotations

import asyncio
import signal
from typing import Any

import aiohttp

from .backend import Ledger, TrailParams, decrypt_secret
from .config import Config, DEFAULT
from .exec import BinanceExecutor, RiskEngine
from .market import AskWallTracker, MomentumSignal, SignalEngine, binance_depth_stream, run_l2_feed


class SniperEngine:
    def __init__(self, cfg: Config, ledger: Ledger) -> None:
        self.cfg = cfg
        self.ledger = ledger
        self._session: aiohttp.ClientSession | None = None
        self._executor: BinanceExecutor | None = None
        self._engines: dict[str, SignalEngine] = {}
        self._tasks: list[asyncio.Task[Any]] = []
        self._last_signals: dict[str, dict[str, Any]] = {}

    def live_state(self) -> dict[str, Any]:
        ex = self._executor
        if not ex:
            return {"positions": {}, "marks": {}}
        unrealized = {}
        for sym, pos in ex.risk.positions.items():
            mark = ex.mark_prices.get(sym, pos.entry)
            pct = ex.risk.unrealized_pct(pos, mark)
            unrealized[sym] = {"entry": pos.entry, "mark": mark, "pnl_pct": round(pct, 6), "qty": pos.qty}
        marks = dict(ex.mark_prices)
        return {
            "positions": unrealized,
            "marks": marks,
            "equity": round(ex.risk.equity(marks), 2),
            "trade_notional": round(ex.risk.equity(marks) * ex.risk.alloc_pct, 2),
            "open_count": len(ex.risk.positions),
            "max_open": ex.risk.max_open,
        }

    def _sync_trail_params(self) -> None:
        p = self.ledger.learner.params
        if self._executor:
            self._executor.risk.apply_params(p.hard_trigger, p.hard_floor, p.wide_trigger, p.wide_floor)

    def _build_signal_engines(self, executor: BinanceExecutor) -> dict[str, SignalEngine]:
        out: dict[str, SignalEngine] = {}

        def emit(sig: MomentumSignal) -> None:
            self._last_signals[sig.symbol] = {
                "kind": sig.kind, "imbalance": sig.imbalance, "mid": sig.mid, "ts_ns": sig.ts_ns,
            }
            asyncio.get_running_loop().create_task(executor.on_signal(sig))

        for sym in self.cfg.symbols:
            out[sym] = SignalEngine(
                symbol=sym, depth_levels=self.cfg.depth_levels,
                imbalance_threshold=self.cfg.imbalance_threshold, max_spread_bps=self.cfg.max_spread_bps,
                wall_tracker=AskWallTracker(
                    consume_window_ns=self.cfg.wall_consume_ms * 1_000_000, qty_mult=self.cfg.wall_qty_mult,
                ),
                on_signal=emit,
                on_update=lambda s, px: asyncio.get_running_loop().create_task(executor.on_mark(s, px))
            )
        return out

    async def _ml_loop(self) -> None:
        while True:
            recent = self.ledger.recent_pnls(30)
            self.ledger.learner.choose(recent)
            self._sync_trail_params()
            await asyncio.sleep(5.0)

    async def start(self) -> None:
        p = self.ledger.learner.params
        timeout = aiohttp.ClientTimeout(total=5, connect=2)
        self._session = aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(limit=32))

        risk = RiskEngine(
            p.hard_trigger, p.hard_floor, p.wide_trigger, p.wide_floor,
            virtual_start=self.cfg.virtual_balance_usdt,
            alloc_pct=self.cfg.capital_allocation_pct,
            max_open=self.cfg.max_open_positions,
            realized_pnl=self.ledger.total_realized_pnl(),
        )
        self._executor = BinanceExecutor(
            session=self._session,
            rest_base=self.cfg.rest_base,
            api_key=decrypt_secret(self.cfg, self.cfg.api_key_enc, self.cfg.api_key_plain),
            api_secret=decrypt_secret(self.cfg, self.cfg.api_secret_enc, self.cfg.api_secret_plain),
            dry_run=self.cfg.dry_run,
            risk=risk,
            on_entry=lambda s, px, lat: self.ledger.record_entry(s, px, lat),
            on_exit=lambda s, px, qty: self.ledger.record_exit(s, px, qty),
        )
        self._engines = self._build_signal_engines(self._executor)
        ws_url = self.cfg.ws_base if "stream?" in self.cfg.ws_base else binance_depth_stream(self.cfg.symbols)
        self._tasks += [
            asyncio.create_task(run_l2_feed(self._session, ws_url, self._engines)),
            asyncio.create_task(self._ml_loop()),
        ]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._session:
            await self._session.close()


async def run_engine(cfg: Config, ledger: Ledger) -> SniperEngine:
    eng = SniperEngine(cfg, ledger)
    await eng.start()
    return eng
