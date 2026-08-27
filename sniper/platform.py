"""Platform launcher — FastAPI (port=0), engine, Next.js (dynamic port), ports manifest."""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

import uvicorn
from uvicorn import Config as UvConfig
from uvicorn.server import Server

from .backend import Ledger, create_api
from .config import DEFAULT, Config
from .engine import SniperEngine, run_engine
from .ports import allocate_tcp_port, write_manifest

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "web"


async def _serve_fastapi(app, host: str) -> tuple[Server, int]:
    cfg = UvConfig(app, host=host, port=0, log_level="warning", access_log=False)
    server = Server(cfg)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    return server, port


def _spawn_web(host: str, port: int, api_url: str) -> subprocess.Popen[Any]:
    env = {**os.environ, "PORT": str(port), "HOST": host, "NEXT_PUBLIC_API_URL": api_url}
    return subprocess.Popen(
        ["npm", "run", "dev", "--", "-p", str(port), "-H", host],
        cwd=WEB, env=env, shell=sys.platform == "win32",
    )


async def main(cfg: Config = DEFAULT) -> None:
    if cfg.uvloop and sys.platform != "win32":
        try:
            import uvloop
            uvloop.install()
        except ImportError:
            pass

    ledger = Ledger(cfg)
    engine: SniperEngine | None = None
    web_proc: subprocess.Popen[Any] | None = None
    api_server: Server | None = None

    try:
        engine = await run_engine(cfg, ledger)
        platform = create_api(cfg, ledger, live_fn=lambda: engine.live_state())
        api_server, api_port = await _serve_fastapi(platform.app, cfg.bind_host)
        web_port = allocate_tcp_port(cfg.bind_host)
        api_url = f"http://{cfg.bind_host}:{api_port}"
        manifest = {"api": {"host": cfg.bind_host, "port": api_port, "url": api_url}, "web": {"host": cfg.bind_host, "port": web_port}}
        write_manifest(cfg.ports_file, manifest)
        pub = WEB / "public"
        pub.mkdir(parents=True, exist_ok=True)
        write_manifest(str(pub / "ports.json"), manifest)
        print(f"[platform] API  -> {api_url}", flush=True)
        if WEB.exists():
            web_proc = _spawn_web(cfg.bind_host, web_port, api_url)
            print(f"[platform] Web  -> http://{cfg.bind_host}:{web_port}", flush=True)

        stop = asyncio.Event()

        def _halt(*_: Any) -> None:
            stop.set()

        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, _halt)
        except NotImplementedError:
            signal.signal(signal.SIGINT, lambda *_: _halt())
            if hasattr(signal, "SIGBREAK"):
                signal.signal(signal.SIGBREAK, lambda *_: _halt())

        await stop.wait()
    finally:
        if web_proc:
            web_proc.terminate()
        if api_server:
            api_server.should_exit = True
        if engine:
            await engine.stop()


def entry() -> None:
    asyncio.run(main())