"""Non-blocking execution + dynamic trailing stop risk engine."""
from __future__ import annotations

import hashlib
import hmac
import time
import urllib.parse
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable

import aiohttp

from .market import MomentumSignal


class TrailPhase(Enum):
    OPEN = auto()
    HARD_FLOOR = auto()
    WIDE_TRAIL = auto()


@dataclass(slots=True)
class Position:
    symbol: str
    qty: float
    entry: float
    entry_ns: int
    phase: TrailPhase = TrailPhase.OPEN
    stop_pct: float = 0.0
    peak_pct: float = 0.0


@dataclass
class RiskEngine:
    hard_trigger: float
    hard_floor: float
    wide_trigger: float
    wide_floor: float
    initial_stop_pct: float = -0.02
    virtual_start: float = 10_000.0
    alloc_pct: float = 0.10
    max_open: int = 5
    realized_pnl: float = 0.0
    positions: dict[str, Position] = field(default_factory=dict)

    def apply_params(self, hard_trigger: float, hard_floor: float, wide_trigger: float, wide_floor: float) -> None:
        self.hard_trigger, self.hard_floor = hard_trigger, hard_floor
        self.wide_trigger, self.wide_floor = wide_trigger, wide_floor

    def equity(self, marks: dict[str, float]) -> float:
        unrealized = sum(
            (marks.get(sym, p.entry) - p.entry) * p.qty for sym, p in self.positions.items()
        )
        return self.virtual_start + self.realized_pnl + unrealized

    def can_open(self) -> bool:
        return len(self.positions) < self.max_open

    def trade_size(self, price: float, marks: dict[str, float]) -> tuple[float, float] | None:
        if not self.can_open() or price <= 0:
            return None
        notional = self.equity(marks) * self.alloc_pct
        if notional <= 0:
            return None
        return round(notional, 2), round(notional / price, 6)

    def apply_exit_pnl(self, pnl_usdt: float) -> None:
        self.realized_pnl += pnl_usdt

    def unrealized_pct(self, pos: Position, mark: float) -> float:
        return (mark - pos.entry) / pos.entry if pos.entry > 0 else 0.0

    def on_mark(self, symbol: str, mark: float) -> bool:
        pos = self.positions.get(symbol)
        if pos is None:
            return False
        pnl = self.unrealized_pct(pos, mark)
        pos.peak_pct = max(pos.peak_pct, pnl)
        if pos.phase is TrailPhase.OPEN and pnl >= self.hard_trigger:
            pos.phase, pos.stop_pct = TrailPhase.HARD_FLOOR, self.hard_floor
        elif pos.phase is TrailPhase.HARD_FLOOR and pnl >= self.wide_trigger:
            pos.phase, pos.stop_pct = TrailPhase.WIDE_TRAIL, self.wide_floor
        elif pos.phase is TrailPhase.WIDE_TRAIL:
            pos.stop_pct = max(pos.stop_pct, pos.peak_pct - (self.wide_trigger - self.wide_floor))
        return pnl <= pos.stop_pct and pos.stop_pct > 0

    def open(self, symbol: str, qty: float, entry: float) -> Position:
        pos = Position(symbol=symbol, qty=qty, entry=entry, entry_ns=time.perf_counter_ns(), stop_pct=self.initial_stop_pct)
        self.positions[symbol] = pos
        return pos

    def close(self, symbol: str) -> Position | None:
        return self.positions.pop(symbol, None)


@dataclass
class BinanceExecutor:
    session: aiohttp.ClientSession
    rest_base: str
    api_key: str
    api_secret: str
    dry_run: bool
    risk: RiskEngine
    on_entry: Callable[[str, float, float], None] | None = None
    on_exit: Callable[[str, float, float], None] | None = None
    mark_prices: dict[str, float] = field(default_factory=dict)

    def _sign(self, params: dict[str, Any]) -> str:
        query = urllib.parse.urlencode(params)
        sig = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        return f"{query}&signature={sig}"

    async def _request(self, method: str, path: str, params: dict[str, Any]) -> dict[str, Any]:
        params["timestamp"] = int(time.time() * 1000)
        url = f"{self.rest_base}{path}?{self._sign(params)}"
        headers = {"X-MBX-APIKEY": self.api_key}
        async with self.session.request(method, url, headers=headers) as resp:
            data = await resp.json()
            if resp.status >= 400:
                raise RuntimeError(f"binance {resp.status}: {data}")
            return data

    async def market_buy(self, signal: MomentumSignal) -> Position | None:
        symbol = signal.symbol
        if symbol in self.risk.positions or not self.risk.can_open():
            return None
        price = signal.best_ask or signal.mid
        sized = self.risk.trade_size(price, self.mark_prices)
        if not sized: return None
        notional, _ = sized
        t0 = time.perf_counter_ns()
        if not self.dry_run:
            res = await self._request(
                "POST", "/api/v3/order",
                {"symbol": symbol, "side": "BUY", "type": "MARKET", "quoteOrderQty": str(notional)},
            )
            # Use actual execution data from Binance
            price = sum(float(f['p']) * float(f['q']) for f in res.get('fills', [])) / sum(float(f['q']) for f in res.get('fills', [])) if res.get('fills') else price
            qty = sum(float(f['q']) for f in res.get('fills', []))
        else:
            qty = sized[1]
        latency_ms = (time.perf_counter_ns() - t0) / 1_000_000
        if self.on_entry:
            self.on_entry(symbol, price, latency_ms)
        return self.risk.open(symbol, qty, price)

    async def market_sell(self, symbol: str) -> None:
        pos = self.risk.positions.get(symbol)
        if not pos: return
        t0 = time.perf_counter_ns()
        if not self.dry_run:
            # Dynamic precision fix: Binance rejects too many decimals
            qty_str = f"{pos.qty:.8f}".rstrip('0').rstrip('.')
            res = await self._request(
                "POST", "/api/v3/order",
                {"symbol": symbol, "side": "SELL", "type": "MARKET", "quantity": qty_str},
            )
            mark = sum(float(f['p']) * float(f['q']) for f in res.get('fills', [])) / sum(float(f['q']) for f in res.get('fills', [])) if res.get('fills') else pos.entry
        else:
            mark = self.mark_prices.get(symbol, pos.entry)
        
        closed = self.risk.close(symbol)
        if closed and self.on_exit:
            self.on_exit(symbol, mark, closed.qty)
            self.risk.apply_exit_pnl((mark - closed.entry) * closed.qty)

    async def on_signal(self, signal: MomentumSignal) -> None:
        await self.market_buy(signal)

    async def on_mark(self, symbol: str, mark: float) -> None:
        self.mark_prices[symbol] = mark
        if self.risk.on_mark(symbol, mark):
            await self.market_sell(symbol)
