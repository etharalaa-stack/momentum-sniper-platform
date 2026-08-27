"""DB ledger, analytics, Q-learning trail optimizer, FastAPI + JWT + rate limits."""
from __future__ import annotations

import json
import math
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from cryptography.fernet import Fernet, InvalidToken
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from sqlmodel import Field, Session, SQLModel, create_engine, select

from .config import Config, DEFAULT

# ── Crypto ──────────────────────────────────────────────────────────────────

def _fernet(key: str) -> Fernet:
    if not key:
        import base64, hashlib
        return Fernet(base64.urlsafe_b64encode(hashlib.sha256(b"dev-fallback-key").digest()))
    if len(key) == 44:
        return Fernet(key.encode())
    import base64, hashlib
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(key.encode()).digest()))


def decrypt_secret(cfg: Config, enc: str, plain: str) -> str:
    if plain:
        return plain
    if not enc or not cfg.master_key:
        return ""
    try:
        return _fernet(cfg.master_key).decrypt(enc.encode()).decode()
    except (InvalidToken, ValueError):
        return ""


def encrypt_secret(key: str, value: str) -> str:
    return _fernet(key).encrypt(value.encode()).decode()


# ── Schema ──────────────────────────────────────────────────────────────────

class Trade(SQLModel, table=True):
    __tablename__: str = "trades"  # إجبار النظام على استخدام الجدول الذي يكتب فيه المحرك
    id: int | None = Field(default=None, primary_key=True)
    symbol: str
    entry_time: datetime
    exit_time: datetime | None = None
    entry_price: float
    exit_price: float | None = None
    pnl_usdt: float | None = None
    pnl_percent: float | None = None
    is_win: bool | None = None
    execution_latency: float = 0.0


class TrailParams(BaseModel):
    hard_trigger: float
    hard_floor: float
    wide_trigger: float
    wide_floor: float


# ── Analytics ───────────────────────────────────────────────────────────────

def analytics(session: Session) -> dict[str, Any]:
    trades = session.exec(select(Trade).where(Trade.exit_time != None)).all()  # noqa: E711
    if not trades:
        return {"total_pnl": 0.0, "win_rate": 0.0, "max_drawdown": 0.0, "count": 0}
    wins = sum(1 for t in trades if t.is_win)
    total = sum(t.pnl_usdt or 0 for t in trades)
    equity, peak, max_dd = 0.0, 0.0, 0.0
    for t in sorted(trades, key=lambda x: x.exit_time or x.entry_time):
        equity += t.pnl_usdt or 0
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return {
        "total_pnl": round(total, 4),
        "win_rate": round(wins / len(trades), 4),
        "max_drawdown": round(max_dd, 4),
        "count": len(trades),
    }


# ── Q-Learning trail optimizer ──────────────────────────────────────────────

ACTIONS: list[TrailParams] = [
    TrailParams(hard_trigger=0.15, hard_floor=0.10, wide_trigger=0.40, wide_floor=0.30),
    TrailParams(hard_trigger=0.18, hard_floor=0.13, wide_trigger=0.45, wide_floor=0.35),
    TrailParams(hard_trigger=0.20, hard_floor=0.15, wide_trigger=0.50, wide_floor=0.40),
    TrailParams(hard_trigger=0.25, hard_floor=0.18, wide_trigger=0.55, wide_floor=0.45),
    TrailParams(hard_trigger=0.30, hard_floor=0.22, wide_trigger=0.60, wide_floor=0.50),
]


class TrailLearner:
    """Epsilon-greedy Q-learning over volatility buckets × trail presets."""

    def __init__(self, cfg: Config, state_path: str | None = None) -> None:
        self.epsilon = cfg.ml_epsilon
        self.q: dict[int, list[float]] = {i: [0.0] * len(ACTIONS) for i in range(3)}
        self._last_state = 1
        self._last_action = 2
        self._path = state_path or str(Path(cfg.db_path).with_name("qtable.json"))
        self.params = ACTIONS[2]
        self._load()

    def _load(self) -> None:
        p = Path(self._path)
        if p.exists():
            data = json.loads(p.read_text())
            self.q = {int(k): v for k, v in data.get("q", {}).items()}
            idx = data.get("action", 2)
            self.params = ACTIONS[idx]
            self._last_action = idx

    def _save(self) -> None:
        Path(self._path).write_text(
            json.dumps({"q": self.q, "action": self._last_action}, separators=(",", ":"))
        )

    @staticmethod
    def _vol_bucket(recent_pnls: list[float]) -> int:
        if len(recent_pnls) < 2:
            return 1
        mean = sum(recent_pnls) / len(recent_pnls)
        var = sum((x - mean) ** 2 for x in recent_pnls) / len(recent_pnls)
        sigma = math.sqrt(var)
        if sigma < 0.02:
            return 0
        if sigma < 0.05:
            return 1
        return 2

    def choose(self, recent_pnls: list[float]) -> TrailParams:
        state = self._vol_bucket(recent_pnls)
        self._last_state = state
        if random.random() < self.epsilon:
            self._last_action = random.randrange(len(ACTIONS))
        else:
            self._last_action = max(range(len(ACTIONS)), key=lambda a: self.q[state][a])
        self.params = ACTIONS[self._last_action]
        return self.params

    def learn(self, reward: float, recent_pnls: list[float], alpha: float = 0.2, gamma: float = 0.85) -> TrailParams:
        s = self._last_state
        a = self._last_action
        self.q[s][a] = (1 - alpha) * self.q[s][a] + alpha * (reward + gamma * max(self.q[s]))
        self._save()
        return self.choose(recent_pnls)


# ── Ledger service ──────────────────────────────────────────────────────────

class Ledger:
    def __init__(self, cfg: Config) -> None:
        self.engine = create_engine(f"sqlite:///{cfg.db_path}", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)
        self.learner = TrailLearner(cfg)
        self._open: dict[str, tuple[int, float]] = {}

    def session(self) -> Session:
        return Session(self.engine)

    def record_entry(self, symbol: str, price: float, latency_ms: float) -> int:
        with self.session() as s:
            t = Trade(
                symbol=symbol,
                entry_time=datetime.now(timezone.utc),
                entry_price=price,
                execution_latency=latency_ms,
            )
            s.add(t)
            s.commit()
            s.refresh(t)
            self._open[symbol] = (t.id, price)
            return t.id

    def record_exit(self, symbol: str, price: float, qty: float) -> Trade | None:
        slot = self._open.pop(symbol, None)
        if slot is None:
            return None
        tid, entry = slot
        pnl_pct = (price - entry) / entry if entry else 0.0
        pnl_usdt = (price - entry) * qty
        is_win = pnl_usdt > 0
        with self.session() as s:
            t = s.get(Trade, tid)
            if not t:
                return None
            t.exit_time = datetime.now(timezone.utc)
            t.exit_price = price
            t.pnl_usdt = round(pnl_usdt, 6)
            t.pnl_percent = round(pnl_pct, 6)
            t.is_win = is_win
            s.add(t)
            s.commit()
            s.refresh(t)
        recent = self.recent_pnls(30)
        self.learner.learn(pnl_pct, recent)
        return t

    def recent_pnls(self, n: int = 30) -> list[float]:
        with self.session() as s:
            rows = s.exec(
                select(Trade.pnl_percent)
                .where(Trade.pnl_percent.is_not(None))
                .order_by(Trade.id.desc())
                .limit(n)
            ).all()
        return [float(x) for x in rows if x is not None]

    def total_realized_pnl(self) -> float:
        with self.session() as s:
            rows = s.exec(select(Trade.pnl_usdt).where(Trade.pnl_usdt.is_not(None))).all()
        return sum(float(x) for x in rows if x is not None)


# ── FastAPI ─────────────────────────────────────────────────────────────────

class LoginBody(BaseModel):
    api_key: str


class PlatformAPI:
    def __init__(self, cfg: Config, ledger: Ledger, live_fn: Callable[[], dict[str, Any]] | None = None) -> None:
        self.cfg = cfg
        self.ledger = ledger
        self.live_fn = live_fn or (lambda: {})
        self.limiter = Limiter(key_func=get_remote_address)
        self.app = FastAPI(title="Sniper Platform", docs_url=None, redoc_url=None)
        self.app.state.limiter = self.limiter
        self.app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )
        self._routes()

    def _token(self, sub: str = "admin") -> str:
        return jwt.encode(
            {"sub": sub, "exp": int(time.time()) + 86400},
            self.cfg.jwt_secret,
            algorithm="HS256",
        )

    def _auth(self, creds: HTTPAuthorizationCredentials | None = Depends(HTTPBearer(auto_error=False))):
        if creds is None:
            raise HTTPException(401, "missing token")
        try:
            jwt.decode(creds.credentials, self.cfg.jwt_secret, algorithms=["HS256"])
        except JWTError as e:
            raise HTTPException(401, "invalid token") from e

    def _routes(self) -> None:
        app, lim, cfg, auth = self.app, self.limiter, self.cfg, self._auth

        @app.post("/auth/login")
        @lim.limit(cfg.rate_limit)
        async def login(body: LoginBody, request: Request):
            if body.api_key != cfg.dashboard_key:
                raise HTTPException(403, "invalid key")
            return {"token": self._token()}

        @app.get("/api/analytics")
        @lim.limit(cfg.rate_limit)
        async def get_analytics(request: Request, _=Depends(auth)):
            with self.ledger.session() as s:
                return analytics(s)

        @app.get("/api/trades")
        @lim.limit(cfg.rate_limit)
        async def get_trades(
            request: Request,
            page: int = Query(1, ge=1),
            limit: int = Query(20, ge=1, le=100),
            _=Depends(auth),
        ):
            offset = (page - 1) * limit
            with self.ledger.session() as s:
                total = len(s.exec(select(Trade)).all())
                rows = s.exec(select(Trade).order_by(Trade.id.desc()).offset(offset).limit(limit)).all()
            return {
                "page": page,
                "limit": limit,
                "total": total,
                "items": [r.model_dump() for r in rows],
            }

        @app.get("/api/live")
        @lim.limit("120/minute")
        async def get_live(request: Request, _=Depends(auth)):
            p = self.ledger.learner.params
            return {
                **self.live_fn(),
                "trail_params": p.model_dump(),
            }

        @app.get("/api/ports")
        async def get_ports():
            pf = Path(cfg.ports_file)
            if pf.exists():
                return json.loads(pf.read_text())
            return {}


def create_api(cfg: Config, ledger: Ledger, live_fn: Callable[[], dict[str, Any]] | None = None) -> PlatformAPI:
    return PlatformAPI(cfg, ledger, live_fn)