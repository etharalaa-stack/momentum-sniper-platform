"""Dynamic port allocation + cross-service manifest."""
from __future__ import annotations

import asyncio
import json
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from aiohttp import web


@dataclass
class ServiceRegistry:
    host: str
    name: str
    port: int = 0
    _routes: dict[str, str] = field(default_factory=dict)

    def endpoint(self, path: str = "") -> str:
        return f"http://{self.host}:{self.port}{path}"

    def register_route(self, label: str, path: str) -> None:
        self._routes[label] = path

    def manifest(self) -> dict[str, Any]:
        return {"service": self.name, "host": self.host, "port": self.port, "routes": dict(self._routes)}


def allocate_tcp_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def write_manifest(path: str, data: dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(data, indent=2))


async def serve(app: web.Application, host: str, registry: ServiceRegistry | None = None, *, port: int = 0):
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    bound = int(site._server.sockets[0].getsockname()[1])  # type: ignore[attr-defined]
    if registry is not None:
        registry.port = bound
    return runner, bound


async def shutdown(runner: web.AppRunner) -> None:
    await runner.cleanup()
