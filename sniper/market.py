"""L2 WebSocket telemetry + sub-ms imbalance / ask-wall consumption signals."""
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque

import aiohttp

try:
    import orjson

    def loads(raw: str | bytes) -> Any:
        return orjson.loads(raw)

except ImportError:
    import json

    def loads(raw: str | bytes) -> Any:
        return json.loads(raw)


@dataclass(slots=True)
class Level:
    price: float
    qty: float


@dataclass(slots=True)
class BookSnapshot:
    ts_ns: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    best_bid: float
    best_ask: float
    mid: float
    spread_bps: float
    bid_qty: float
    ask_qty: float
    imbalance: float  # (bid-ask)/(bid+ask)


@dataclass(slots=True)
class MomentumSignal:
    symbol: str
    ts_ns: int
    kind: str  # "imbalance" | "wall_consume"
    imbalance: float
    mid: float
    best_ask: float
    meta: dict[str, float] = field(default_factory=dict)


def _parse_levels(raw: list[list[str]], n: int) -> tuple[Level, ...]:
    out: list[Level] = []
    for row in raw[:n]:
        out.append(Level(float(row[0]), float(row[1])))
    return tuple(out)


def _snapshot(symbol: str, bids: tuple[Level, ...], asks: tuple[Level, ...]) -> BookSnapshot:
    best_bid = bids[0].price if bids else 0.0
    best_ask = asks[0].price if asks else 0.0
    mid = (best_bid + best_ask) * 0.5 if best_bid and best_ask else 0.0
    spread_bps = ((best_ask - best_bid) / mid * 10_000.0) if mid else 9999.0
    bid_qty = sum(l.qty for l in bids)
    ask_qty = sum(l.qty for l in asks)
    denom = bid_qty + ask_qty
    imbalance = (bid_qty - ask_qty) / denom if denom else 0.0
    return BookSnapshot(
        ts_ns=time.perf_counter_ns(),
        bids=bids,
        asks=asks,
        best_bid=best_bid,
        best_ask=best_ask,
        mid=mid,
        spread_bps=spread_bps,
        bid_qty=bid_qty,
        ask_qty=ask_qty,
        imbalance=imbalance,
    )


@dataclass
class AskWallTracker:
    """Detect sudden consumption of dominant ask walls."""

    consume_window_ns: int
    qty_mult: float
    _history: Deque[tuple[int, tuple[Level, ...]]] = field(default_factory=lambda: deque(maxlen=64))

    def update(self, ts_ns: int, asks: tuple[Level, ...]) -> float | None:
        self._history.append((ts_ns, asks))
        if len(asks) < 3 or len(self._history) < 2:
            return None
        med = sorted(l.qty for l in asks)[len(asks) // 2]
        wall = next((l for l in asks[:5] if l.qty >= med * self.qty_mult), None)
        if wall is None:
            return None
        cutoff = ts_ns - self.consume_window_ns
        for past_ts, past_asks in reversed(self._history):
            if past_ts < cutoff:
                break
            match = next((l for l in past_asks if abs(l.price - wall.price) < 1e-12), None)
            if match and match.qty >= wall.qty * 0.85:
                return None
        return wall.price


@dataclass
class SignalEngine:
    symbol: str
    depth_levels: int
    imbalance_threshold: float
    max_spread_bps: float
    wall_tracker: AskWallTracker
    on_signal: Callable[[MomentumSignal], None]
    on_update: Callable[[str, float], None] | None = None
    _last_emit_ns: int = 0
    cooldown_ns: int = 50_000_000  # 50ms anti-chatter

    def on_book(self, bids_raw: list, asks_raw: list) -> None:
        bids = _parse_levels(bids_raw, self.depth_levels)
        asks = _parse_levels(asks_raw, self.depth_levels)
        snap = _snapshot(self.symbol, bids, asks)
        now = snap.ts_ns

        if self.on_update:
            self.on_update(self.symbol, snap.mid)

        wall_px = self.wall_tracker.update(now, asks)

        if wall_px is not None and now - self._last_emit_ns >= self.cooldown_ns:
            self._last_emit_ns = now
            self.on_signal(
                MomentumSignal(
                    symbol=self.symbol,
                    ts_ns=now,
                    kind="wall_consume",
                    imbalance=snap.imbalance,
                    mid=snap.mid,
                    best_ask=snap.best_ask,
                    meta={"wall_price": wall_px},
                )
            )
            return

        if (
            snap.spread_bps <= self.max_spread_bps
            and snap.imbalance >= self.imbalance_threshold
            and now - self._last_emit_ns >= self.cooldown_ns
        ):
            self._last_emit_ns = now
            self.on_signal(
                MomentumSignal(
                    symbol=self.symbol,
                    ts_ns=now,
                    kind="imbalance",
                    imbalance=snap.imbalance,
                    mid=snap.mid,
                    best_ask=snap.best_ask,
                    meta={"bid_qty": snap.bid_qty, "ask_qty": snap.ask_qty},
                )
            )


def binance_depth_stream(symbols: tuple[str, ...]) -> str:
    streams = "/".join(f"{s.lower()}@depth20@100ms" for s in symbols)
    return f"wss://stream.binance.com:9443/stream?streams={streams}"


async def run_l2_feed(
    session: aiohttp.ClientSession,
    ws_url: str,
    engines: dict[str, SignalEngine],
    *,
    reconnect_delay: float = 0.25,
) -> None:
    """Non-blocking L2 multiplexer — hot path avoids await except I/O boundaries."""
    while True:
        try:
            async with session.ws_connect(ws_url, heartbeat=20, compress=0) as ws:
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    payload = loads(msg.data)
                    data = payload.get("data") or payload
                    symbol = str(data.get("s", "")).upper()
                    engine = engines.get(symbol)
                    if engine is None:
                        continue
                    engine.on_book(data.get("b", []), data.get("a", []))
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(reconnect_delay)
