#!/usr/bin/env python3
"""Momentum Sniper Engine — cross-venue velocity, imbalance & breakout (Frankfurt VPS)."""
from __future__ import annotations

import asyncio
import json
import math
import os
import signal
import sqlite3
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Deque

import aiohttp
from aiohttp.resolver import ThreadedResolver

try:
    import orjson

    def _loads(raw: str | bytes) -> Any:
        return orjson.loads(raw)

except ImportError:

    def _loads(raw: str | bytes) -> Any:
        return json.loads(raw)

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "TAOUSDT")
GATE_PAIR = {s: f"{s[:-4]}_{s[-4:]}" for s in SYMBOLS}
BINANCE_REST = "https://api.binance.com/api/v3/ticker/bookTicker"
GATE_REST = "https://api.gateio.ws/api/v4/spot/tickers"
BINANCE_WS = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{s.lower()}@bookTicker" for s in SYMBOLS
)
GATE_WS = "wss://api.gateio.ws/ws/v4/"
DB_PATH = Path(os.getenv("SNIPER_DB", str(Path(__file__).resolve().parent / "sniper.db")))
FEE_PCT = 0.04
BASE_MOMENTUM_SCORE = float(os.getenv("MOM_BASE_SCORE", "2.0"))
NOTIONAL_USDT = float(os.getenv("SNIPER_QUOTE_SIZE", "1000"))
MIN_BOOK_DEPTH_USDT = float(os.getenv("ARB_MIN_DEPTH_USDT", "300"))
MAX_POSITION_SEC = float(os.getenv("MOM_MAX_HOLD_SEC", "300"))
TRADE_COOLDOWN_SEC = 30.0
IDLE_RELAX_WINDOW_SEC = float(os.getenv("ARB_IDLE_RELAX_SEC", "900"))
IDLE_RELAX_MAX_SCORE = float(os.getenv("MOM_IDLE_RELAX", "1.2"))
PRINT_INTERVAL_SEC = 0.5
PEAK_HOURS_UTC = frozenset({7, 8, 9, 13, 14, 15, 16, 20, 21, 22})
MEME_SYMBOLS: frozenset[str] = frozenset()  # e.g. {"DOGEUSDT"} — stricter depth when set
MEME_DEPTH_MULT = 2.5
REGIME_LOG_SEC = 60.0
VOL_WINDOW = 64
DRY_RUN = os.getenv("SNIPER_DRY_RUN", "1") == "1"


@dataclass(slots=True)
class TopOfBook:
    bid: float = 0.0
    ask: float = 0.0
    bid_qty: float = 0.0
    ask_qty: float = 0.0
    ts_ns: int = 0


@dataclass
class BookState:
    binance: dict[str, TopOfBook] = field(default_factory=lambda: {s: TopOfBook() for s in SYMBOLS})
    gate: dict[str, TopOfBook] = field(default_factory=lambda: {s: TopOfBook() for s in SYMBOLS})
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(slots=True)
class TradeSignal:
    symbol: str
    side: str
    entry_price: float
    mid: float
    strength: float
    raw_score: float
    latency_ms: float


@dataclass(slots=True)
class OpenPosition:
    tid: int
    symbol: str
    side: str
    entry: float
    entry_mono: float
    tp_pct: float
    sl_pct: float


@dataclass
class SymStats:
    wins: int = 0
    trades: int = 0
    cooldown: float = TRADE_COOLDOWN_SEC
    thresh_bias: float = 0.0
    nets: Deque[float] = field(default_factory=lambda: deque(maxlen=40))
    lats: Deque[float] = field(default_factory=lambda: deque(maxlen=40))


class MomentumTracker:
    __slots__ = ("_mids", "_qtys")

    def __init__(self) -> None:
        self._mids: dict[str, Deque[tuple[float, float]]] = {
            s: deque(maxlen=32) for s in SYMBOLS
        }
        self._qtys: dict[str, Deque[float]] = {s: deque(maxlen=16) for s in SYMBOLS}

    def push(self, symbol: str, mid: float, total_bid: float, total_ask: float) -> None:
        if mid <= 0:
            return
        self._mids[symbol].append((time.monotonic(), mid))
        self._qtys[symbol].append(total_bid + total_ask)

    def velocity_bps(self, symbol: str) -> float:
        xs = self._mids[symbol]
        if len(xs) < 3:
            return 0.0
        t0, p0 = xs[0]
        t1, p1 = xs[-1]
        dt = t1 - t0
        if dt < 0.05 or p0 <= 0:
            return 0.0
        return (p1 - p0) / p0 * 10000.0 / dt

    def qty_surge(self, symbol: str) -> float:
        qs = self._qtys[symbol]
        if len(qs) < 4:
            return 0.0
        tail = list(qs)[-2:]
        head = list(qs)[:-2]
        recent = sum(tail) / len(tail)
        base = sum(head) / len(head)
        if base <= 0:
            return 0.0
        return max(0.0, recent / base - 1.0)

    @staticmethod
    def imbalance(bn: TopOfBook, gt: TopOfBook) -> float:
        def imb(b: TopOfBook) -> float:
            t = b.bid_qty + b.ask_qty
            return (b.bid_qty - b.ask_qty) / t if t > 0 else 0.0
        return (imb(bn) + imb(gt)) / 2.0


class VolTracker:
    __slots__ = ("_mids",)

    def __init__(self) -> None:
        self._mids: dict[str, Deque[float]] = {s: deque(maxlen=VOL_WINDOW) for s in SYMBOLS}

    def push(self, symbol: str, mid: float) -> None:
        if mid > 0:
            self._mids[symbol].append(mid)

    def sigma(self, symbol: str) -> float:
        xs = self._mids[symbol]
        if len(xs) < 4:
            return 0.0
        mean = sum(xs) / len(xs)
        var = sum((x - mean) ** 2 for x in xs) / len(xs)
        return math.sqrt(var) / mean if mean else 0.0


class SymbolLearner:
    __slots__ = ("_stats",)

    def __init__(self) -> None:
        self._stats: dict[str, SymStats] = {s: SymStats() for s in SYMBOLS}

    def _s(self, symbol: str) -> SymStats:
        return self._stats[symbol]

    def win_rate(self, symbol: str) -> float:
        st = self._s(symbol)
        return st.wins / st.trades if st.trades else 0.5

    def avg_net(self, symbol: str) -> float:
        st = self._s(symbol)
        return sum(st.nets) / len(st.nets) if st.nets else 0.0

    def avg_latency(self, symbol: str) -> float:
        st = self._s(symbol)
        return sum(st.lats) / len(st.lats) if st.lats else 0.0

    def cooldown(self, symbol: str) -> float:
        return self._s(symbol).cooldown

    def threshold_bias(self, symbol: str) -> float:
        return self._s(symbol).thresh_bias

    def trade_count(self, symbol: str) -> int:
        return self._s(symbol).trades

    def on_trade(self, symbol: str, net_pct: float, latency_ms: float, is_win: bool) -> None:
        st = self._s(symbol)
        st.trades += 1
        if is_win:
            st.wins += 1
        st.nets.append(net_pct)
        st.lats.append(latency_ms)
        wr = st.wins / st.trades
        lat = sum(st.lats) / len(st.lats)
        if wr > 0.6 and lat < 80:
            st.cooldown = max(1.0, st.cooldown * 0.92)
        elif wr < 0.4 or lat > 200:
            st.cooldown = min(90.0, st.cooldown * 1.12)
        if wr < 0.45:
            st.thresh_bias = min(0.03, st.thresh_bias + 0.001)
        elif wr > 0.65:
            st.thresh_bias = max(-0.04, st.thresh_bias - 0.003)


class AdaptiveThreshold:
    __slots__ = ("base",)

    def __init__(self, base: float = BASE_MOMENTUM_SCORE) -> None:
        self.base = base

    @staticmethod
    def _idle_relax(idle_sec: float, weight: float = 1.0) -> float:
        half = IDLE_RELAX_WINDOW_SEC * 0.5
        if idle_sec <= half:
            return 0.0
        progress = min(1.0, (idle_sec - half) / half)
        extra = max(0.0, idle_sec - IDLE_RELAX_WINDOW_SEC) / IDLE_RELAX_WINDOW_SEC
        return min(IDLE_RELAX_MAX_SCORE, (progress + extra * 0.5) * IDLE_RELAX_MAX_SCORE) * weight

    def for_symbol(
        self,
        symbol: str,
        vol: float,
        hour_utc: int,
        learner: SymbolLearner,
        idle_sec: float = 0.0,
        global_idle_sec: float = 0.0,
    ) -> float:
        t = self.base + learner.threshold_bias(symbol) * 40.0
        t += max(0.0, 0.5 - learner.win_rate(symbol)) * 1.5
        t += min(vol, 0.05) * 12.0
        if hour_utc in PEAK_HOURS_UTC:
            t -= 0.4
        avg = learner.avg_net(symbol)
        if avg > 0.04:
            t -= 0.5
        elif avg < 0:
            t += 0.6
        t -= self._idle_relax(idle_sec, 0.65)
        t -= self._idle_relax(global_idle_sec, 0.35)
        return max(0.3, min(12.0, t))


def _momentum_signal(
    symbol: str, bn: TopOfBook, gt: TopOfBook, mom: MomentumTracker,
) -> TradeSignal | None:
    if min(bn.bid, bn.ask, gt.bid, gt.ask) <= 0:
        return None
    bn_mid = (bn.bid + bn.ask) * 0.5
    gt_mid = (gt.bid + gt.ask) * 0.5
    mid = (bn_mid + gt_mid) * 0.5
    mom.push(symbol, mid, bn.bid_qty + gt.bid_qty, bn.ask_qty + gt.ask_qty)
    vel = mom.velocity_bps(symbol)
    imb = MomentumTracker.imbalance(bn, gt)
    surge = mom.qty_surge(symbol)
    align = max(0.5, 1.0 - abs(bn_mid - gt_mid) / mid * 50.0)
    raw = (vel * 0.35 + imb * 18.0 + surge * 8.0) * align
    if abs(raw) < 0.05:
        return None
    side = "LONG" if raw > 0 else "SHORT"
    entry = bn.ask if side == "LONG" else bn.bid
    return TradeSignal(symbol, side, entry, mid, abs(raw), raw, 0.0)


def _liquidity_ok(symbol: str, side: str, bn: TopOfBook, gt: TopOfBook) -> tuple[bool, str]:
    need = MIN_BOOK_DEPTH_USDT * (MEME_DEPTH_MULT if symbol in MEME_SYMBOLS else 1.0)
    if side == "LONG":
        depth = min(bn.ask_qty * bn.ask, gt.ask_qty * gt.ask)
    else:
        depth = min(bn.bid_qty * bn.bid, gt.bid_qty * gt.bid)
    if depth < need:
        return False, f"thin({depth:.0f}<{need:.0f})"
    return True, ""


def _tp_sl(vol: float) -> tuple[float, float]:
    tp = max(0.08, min(0.40, 0.12 + vol * 400.0))
    sl = max(0.06, min(0.28, 0.08 + vol * 250.0))
    return tp, sl


class TradeStore:
    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                entry_time DATETIME NOT NULL,
                exit_time DATETIME,
                entry_price REAL NOT NULL,
                exit_price REAL,
                pnl_usdt REAL,
                pnl_percent REAL,
                is_win BOOLEAN,
                execution_latency REAL NOT NULL
            )"""
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS market_regimes (
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
            )"""
        )
        self._conn.commit()

    def log_regime(
        self,
        symbol: str | None,
        hour_utc: int,
        regime: str,
        volatility: float,
        avg_spread: float,
        trade_count: int,
        win_rate: float,
        score: float,
    ) -> None:
        self._conn.execute(
            """INSERT INTO market_regimes
               (recorded_at, symbol, hour_utc, regime, volatility, avg_spread,
                trade_count, win_rate, performance_score)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now(timezone.utc),
                symbol,
                hour_utc,
                regime,
                round(volatility, 8),
                round(avg_spread, 6),
                trade_count,
                round(win_rate, 4),
                round(score, 6),
            ),
        )
        self._conn.commit()

    def insert_entry(self, symbol: str, entry_price: float, latency_ms: float) -> int:
        now = datetime.now(timezone.utc)
        cur = self._conn.execute(
            """INSERT INTO trades
               (symbol, entry_time, entry_price, execution_latency)
               VALUES (?, ?, ?, ?)""",
            (symbol, now, entry_price, round(latency_ms, 3)),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def close_trade(self, tid: int, exit_price: float, side: str) -> float:
        now = datetime.now(timezone.utc)
        row = self._conn.execute(
            "SELECT entry_price FROM trades WHERE id=?", (tid,),
        ).fetchone()
        if not row:
            return 0.0
        entry = float(row[0])
        if side == "LONG":
            pnl_ratio = (exit_price - entry) / entry - FEE_PCT / 100.0
        else:
            pnl_ratio = (entry - exit_price) / entry - FEE_PCT / 100.0
        pnl_usdt = round(NOTIONAL_USDT * pnl_ratio, 6)
        net_pct = pnl_ratio * 100.0
        self._conn.execute(
            """UPDATE trades SET exit_time=?, exit_price=?, pnl_usdt=?,
               pnl_percent=?, is_win=? WHERE id=?""",
            (
                now, exit_price, pnl_usdt, round(pnl_ratio, 8),
                1 if pnl_usdt > 0 else 0, tid,
            ),
        )
        self._conn.commit()
        return net_pct


class MomentumScanner:
    def __init__(
        self,
        books: BookState,
        store: TradeStore,
        adaptive: AdaptiveThreshold,
        learner: SymbolLearner,
        vol: VolTracker,
        mom: MomentumTracker,
    ) -> None:
        self.books = books
        self.store = store
        self.adaptive = adaptive
        self.learner = learner
        self.vol = vol
        self.mom = mom
        self._positions: dict[str, OpenPosition] = {}
        self._last_print: dict[str, float] = {}
        self._last_trade: dict[str, float] = {}
        self._last_global_trade = time.monotonic()
        self._start_mono = time.monotonic()
        self._total_trades = 0
        self._reject_liq = 0

    def _global_idle(self, now: float) -> float:
        return now - self._last_global_trade

    def _check_exit(self, symbol: str, mid: float, latency_ms: float) -> None:
        pos = self._positions.get(symbol)
        if not pos:
            return
        chg = (mid - pos.entry) / pos.entry * 100.0
        if pos.side == "SHORT":
            chg = -chg
        reason = ""
        if chg >= pos.tp_pct:
            reason = "TP"
        elif chg <= -pos.sl_pct:
            reason = "SL"
        elif time.monotonic() - pos.entry_mono >= MAX_POSITION_SEC:
            reason = "TIME"
        if not reason:
            return
        net_pct = self.store.close_trade(pos.tid, mid, pos.side)
        is_win = net_pct > 0
        self.learner.on_trade(symbol, net_pct, latency_ms, is_win)
        self._total_trades += 1
        self._last_trade[symbol] = time.monotonic()
        self._last_global_trade = time.monotonic()
        del self._positions[symbol]
        print(
            f"[DRY-RUN] CLOSE #{pos.tid} {symbol} {pos.side} {reason} "
            f"entry={pos.entry:.6g} exit={mid:.6g} net={net_pct:+.3f}% "
            f"pnl~${NOTIONAL_USDT * net_pct / 100:.2f}",
            flush=True,
        )

    async def on_update(self, symbol: str, latency_ms: float) -> None:
        async with self.books.lock:
            bn, gt = self.books.binance[symbol], self.books.gate[symbol]
            if bn.bid and bn.ask:
                self.vol.push(symbol, (bn.bid + bn.ask) * 0.5)
            sig = _momentum_signal(symbol, bn, gt, self.mom)
            if sig is None:
                return
            sig.latency_ms = latency_ms
            hour = datetime.now(timezone.utc).hour
            vol = self.vol.sigma(symbol)
            now = time.monotonic()
            if symbol in self._positions:
                self._check_exit(symbol, sig.mid, latency_ms)
                return
            idle_sec = now - self._last_trade.get(symbol, self._start_mono)
            global_idle = self._global_idle(now)
            min_score = self.adaptive.for_symbol(
                symbol, vol, hour, self.learner, idle_sec, global_idle,
            )
            liq_ok, liq_reason = _liquidity_ok(symbol, sig.side, bn, gt)
            if now - self._last_print.get(symbol, 0) >= PRINT_INTERVAL_SEC:
                self._last_print[symbol] = now
                regime = "peak" if hour in PEAK_HOURS_UTC else "off"
                if vol > 0.003:
                    regime = "high_vol"
                relax = AdaptiveThreshold._idle_relax(idle_sec, 0.65) + AdaptiveThreshold._idle_relax(
                    global_idle, 0.35,
                )
                print(
                    f"[{symbol}] {sig.side} score={sig.strength:.2f} thr={min_score:.2f} "
                    f"vel={self.mom.velocity_bps(symbol):+.1f}bps imb={MomentumTracker.imbalance(bn, gt):+.2f} "
                    f"{regime} vol={vol:.4f} relax={relax:.2f} | "
                    f"BN {bn.bid:.6g}/{bn.ask:.6g} GT {gt.bid:.6g}/{gt.ask:.6g} | "
                    f"lat={latency_ms:.1f}ms cd={self.learner.cooldown(symbol):.1f}s "
                    f"liq={'OK' if liq_ok else liq_reason}",
                    flush=True,
                )
            if sig.strength < min_score:
                return
            if not liq_ok:
                self._reject_liq += 1
                return
            cd = self.learner.cooldown(symbol)
            if hour in PEAK_HOURS_UTC:
                cd *= 0.85
            if now - self._last_trade.get(symbol, 0) < cd:
                return
            if not DRY_RUN:
                return
            tp, sl = _tp_sl(vol)
            tid = self.store.insert_entry(symbol, sig.entry_price, latency_ms)
            self._positions[symbol] = OpenPosition(
                tid, symbol, sig.side, sig.entry_price, now, tp, sl,
            )
            self._last_trade[symbol] = now
            print(
                f"[DRY-RUN] OPEN #{tid} {symbol} {sig.side} entry={sig.entry_price:.6g} "
                f"score={sig.strength:.2f} TP={tp:.2f}% SL={sl:.2f}% lat={latency_ms:.1f}ms",
                flush=True,
            )


async def _rest_bootstrap(session: aiohttp.ClientSession, books: BookState) -> None:
    t0 = time.perf_counter()

    async def binance(sym: str) -> None:
        async with session.get(BINANCE_REST, params={"symbol": sym}) as r:
            d = await r.json()
            books.binance[sym] = TopOfBook(
                float(d["bidPrice"]), float(d["askPrice"]),
                float(d.get("bidQty", 0)), float(d.get("askQty", 0)), time.perf_counter_ns(),
            )

    async def gate(sym: str) -> None:
        pair = GATE_PAIR[sym]
        async with session.get(GATE_REST, params={"currency_pair": pair}) as r:
            rows = await r.json()
            d = rows[0]
            books.gate[sym] = TopOfBook(
                float(d["highest_bid"]), float(d["lowest_ask"]),
                float(d.get("highest_size", 0)), float(d.get("lowest_size", 0)), time.perf_counter_ns(),
            )

    await asyncio.gather(*[binance(s) for s in SYMBOLS], *[gate(s) for s in SYMBOLS])
    print(f"[boot] REST snapshot loaded in {(time.perf_counter() - t0) * 1000:.0f}ms", flush=True)


async def _binance_ws(session: aiohttp.ClientSession, books: BookState, on_tick: Callable[[str, float], Any]) -> None:
    while True:
        try:
            async with session.ws_connect(BINANCE_WS, heartbeat=20, compress=0) as ws:
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    t0 = time.perf_counter()
                    payload = _loads(msg.data)
                    data = payload.get("data") or payload
                    sym = str(data.get("s", "")).upper()
                    if sym not in books.binance:
                        continue
                    async with books.lock:
                        books.binance[sym] = TopOfBook(
                            float(data["b"]), float(data["a"]),
                            float(data.get("B", 0)), float(data.get("A", 0)), time.perf_counter_ns(),
                        )
                    await on_tick(sym, (time.perf_counter() - t0) * 1000)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[binance-ws] reconnect: {exc}", flush=True)
            await asyncio.sleep(0.5)


async def _gate_ws(session: aiohttp.ClientSession, books: BookState, on_tick: Callable[[str, float], Any]) -> None:
    sub = {
        "time": int(time.time()),
        "channel": "spot.book_ticker",
        "event": "subscribe",
        "payload": [GATE_PAIR[s] for s in SYMBOLS],
    }
    rev = {v: k for k, v in GATE_PAIR.items()}
    while True:
        try:
            async with session.ws_connect(GATE_WS, heartbeat=20, compress=0) as ws:
                await ws.send_json(sub)
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    t0 = time.perf_counter()
                    data = _loads(msg.data)
                    if data.get("channel") == "spot.ping":
                        data["channel"] = "spot.pong"
                        data["event"] = data.get("event") or ""
                        await ws.send_json(data)
                        continue
                    if data.get("channel") != "spot.book_ticker" or data.get("event") != "update":
                        continue
                    res = data.get("result") or {}
                    sym = rev.get(str(res.get("s", "")))
                    if not sym:
                        continue
                    async with books.lock:
                        books.gate[sym] = TopOfBook(
                            float(res["b"]), float(res["a"]),
                            float(res.get("B", 0)), float(res.get("A", 0)), time.perf_counter_ns(),
                        )
                    await on_tick(sym, (time.perf_counter() - t0) * 1000)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[gate-ws] reconnect: {exc}", flush=True)
            await asyncio.sleep(0.5)


async def _rest_poll(session: aiohttp.ClientSession, books: BookState, scanner: MomentumScanner) -> None:
    """Periodic REST refresh as WebSocket backstop."""
    while True:
        await asyncio.sleep(15.0)
        try:
            t0 = time.perf_counter()
            await _rest_bootstrap(session, books)
            lat = (time.perf_counter() - t0) * 1000
            for sym in SYMBOLS:
                await scanner.on_update(sym, lat)
        except Exception as exc:
            print(f"[rest-poll] error: {exc}", flush=True)


async def _regime_loop(scanner: MomentumScanner, store: TradeStore) -> None:
    while True:
        await asyncio.sleep(REGIME_LOG_SEC)
        hour = datetime.now(timezone.utc).hour
        for sym in SYMBOLS:
            vol = scanner.vol.sigma(sym)
            wr = scanner.learner.win_rate(sym)
            avg = scanner.learner.avg_net(sym)
            lat = scanner.learner.avg_latency(sym)
            idle = time.monotonic() - scanner._last_trade.get(sym, scanner._start_mono)
            g_idle = scanner._global_idle(time.monotonic())
            regime = "high_vol" if vol > 0.003 else ("peak" if hour in PEAK_HOURS_UTC else "normal")
            if lat > 120:
                regime = f"{regime}_slow"
            if idle > IDLE_RELAX_WINDOW_SEC * 0.5:
                regime = f"{regime}_idle"
            if g_idle > IDLE_RELAX_WINDOW_SEC * 0.5:
                regime = f"{regime}_global_idle"
            score = avg * wr * (1.2 if hour in PEAK_HOURS_UTC else 1.0) - vol * 100 - lat * 0.002
            store.log_regime(sym, hour, regime, vol, avg, scanner.learner.trade_count(sym), wr, score)
        # aggregate hour snapshot (symbol=NULL)
        agg_vol = sum(scanner.vol.sigma(s) for s in SYMBOLS) / len(SYMBOLS)
        agg_wr = sum(scanner.learner.win_rate(s) for s in SYMBOLS) / len(SYMBOLS)
        store.log_regime(None, hour, "aggregate", agg_vol, 0.0, scanner._total_trades, agg_wr, agg_wr - agg_vol)


async def run() -> None:
    mode = "dry-run" if DRY_RUN else "observe-only"
    print(
        f"[momentum] {mode} | symbols={len(SYMBOLS)} | base_score={BASE_MOMENTUM_SCORE} "
        f"depth>={MIN_BOOK_DEPTH_USDT:.0f}USDT | db={DB_PATH}",
        flush=True,
    )
    books = BookState()
    store = TradeStore(DB_PATH)
    adaptive = AdaptiveThreshold()
    learner = SymbolLearner()
    vol = VolTracker()
    mom = MomentumTracker()
    scanner = MomentumScanner(books, store, adaptive, learner, vol, mom)
    timeout = aiohttp.ClientTimeout(total=8, connect=3)
    connector = aiohttp.TCPConnector(limit=24, ttl_dns_cache=300, resolver=ThreadedResolver())

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        await _rest_bootstrap(session, books)
        for sym in SYMBOLS:
            await scanner.on_update(sym, 0.0)

        stop = asyncio.Event()

        def _halt(*_: Any) -> None:
            stop.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _halt)
            except NotImplementedError:
                signal.signal(sig, lambda *_: _halt())

        tasks = [
            asyncio.create_task(_binance_ws(session, books, scanner.on_update)),
            asyncio.create_task(_gate_ws(session, books, scanner.on_update)),
            asyncio.create_task(_rest_poll(session, books, scanner)),
            asyncio.create_task(_regime_loop(scanner, store)),
        ]
        await stop.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    print(
        f"[momentum] stopped — closed={scanner._total_trades} open={len(scanner._positions)} "
        f"liq_rejects={scanner._reject_liq}",
        flush=True,
    )


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        sys.exit(0)