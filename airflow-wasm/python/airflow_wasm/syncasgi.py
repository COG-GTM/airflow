# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Call an async ASGI app from synchronous code, without threads or an event loop.

Airflow's task runner is synchronous and talks to the Execution API through a sync ``httpx`` client.
Upstream bridges that gap with ``a2wsgi``, which runs an event loop in a second thread -- impossible
in Pyodide.  Here it is unnecessary: nothing in the request cycle actually waits for I/O (PGlite
blocks, the "network" is a function call), so the coroutine can be stepped to completion by hand.

If a coroutine really does suspend on the event loop, ``drive`` raises instead of silently resuming
it with ``None`` and corrupting its state.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit


class WouldBlock(RuntimeError):
    """Raised when a coroutine suspends: there is no event loop here to hand it back to."""


def drive(coro: Any) -> Any:
    """Step a coroutine to completion, refusing to resume it if it suspends."""
    try:
        while True:
            yielded = coro.send(None)
            if yielded is not None:
                raise WouldBlock(f"coroutine suspended on {yielded!r}; cannot be driven synchronously")
    except StopIteration as stop:
        return stop.value


class VersionPicker:
    """
    Pure-ASGI stand-in for Cadwyn's ``VersionPickingMiddleware``.

    Cadwyn routes on a context variable that its middleware sets from the ``airflow-api-version``
    header, so it cannot simply be dropped -- without it every request 404s.  The logic itself is
    trivial; only its ``BaseHTTPMiddleware`` base class is a problem.
    """

    def __init__(
        self,
        app: Any,
        *,
        api_version_parameter_name: str,
        api_version_manager: Any,
        api_version_var: Any,
        api_version_default_value: Any,
    ) -> None:
        self.app = app
        self.parameter_name = api_version_parameter_name
        self.manager = api_version_manager
        self.version_var = api_version_var
        self.default_value = api_version_default_value

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        from cadwyn._internal.context_vars import DEFAULT_API_VERSION_VAR
        from fastapi import Request

        api_version = self.manager.get(Request(scope))
        if api_version is None:
            if callable(self.default_value):
                api_version = await self.default_value(Request(scope))
            else:
                api_version = self.default_value
            DEFAULT_API_VERSION_VAR.set(api_version)
        self.version_var.set(api_version)

        async def send_with_version(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start" and api_version is not None:
                message.setdefault("headers", []).append(
                    (self.parameter_name.encode(), str(api_version).encode())
                )
            await send(message)

        await self.app(scope, receive, send_with_version)


def make_synchronous(app: Any) -> list[str]:
    """
    Rebuild ``app``'s middleware stack so it can be driven without an event loop.

    ``BaseHTTPMiddleware`` runs the rest of the stack as a sibling anyio task and reads the response
    back over a memory stream, which needs a loop.  Cadwyn's version picker is swapped for the
    equivalent above; the Execution API's other two layers (correlation-id logging, token refresh)
    are dropped, as neither means anything for a client in the same interpreter holding a
    non-expiring token.  Returns the names of the layers that were dropped.
    """
    from cadwyn.middleware import VersionPickingMiddleware
    from starlette.middleware import Middleware
    from starlette.middleware.base import BaseHTTPMiddleware

    kept, dropped, changed = [], [], False
    for middleware in app.user_middleware:
        cls = getattr(middleware, "cls", None)
        if isinstance(cls, type) and issubclass(cls, VersionPickingMiddleware):
            kept.append(Middleware(VersionPicker, **middleware.kwargs))
            changed = True
        elif isinstance(cls, type) and issubclass(cls, BaseHTTPMiddleware):
            dropped.append(cls.__name__)
            changed = True
        else:
            kept.append(middleware)

    if changed:
        app.user_middleware = kept
        app.middleware_stack = app.build_middleware_stack()
    return dropped


def request(app: Any, method: str, url: str, headers: Any = None, body: bytes = b"") -> dict[str, Any]:
    """Perform one synchronous request against an ASGI app."""
    split = urlsplit(url)
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method.upper(),
        "scheme": split.scheme or "http",
        "path": split.path or "/",
        "raw_path": (split.path or "/").encode(),
        "query_string": (split.query or "").encode(),
        "root_path": "",
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or [])],
        "client": ("127.0.0.1", 0),
        "server": (split.hostname or "localhost", split.port or 80),
        "state": {},
    }

    sent = False

    async def receive() -> dict[str, Any]:
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    status = 500
    response_headers: list[tuple[bytes, bytes]] = []
    chunks = bytearray()

    async def send(message: dict[str, Any]) -> None:
        nonlocal status
        if message["type"] == "http.response.start":
            status = message["status"]
            response_headers.extend(message.get("headers", []))
        elif message["type"] == "http.response.body":
            chunks.extend(message.get("body", b""))

    drive(app(scope, receive, send))
    return {"status": status, "headers": response_headers, "body": bytes(chunks)}


def httpx_transport(app: Any) -> Any:
    """Make a sync ``httpx`` transport that dispatches into ``app`` on the calling stack."""
    import httpx

    def handler(req: httpx.Request) -> httpx.Response:
        result = request(
            app,
            req.method,
            str(req.url),
            headers=[(k, v) for k, v in req.headers.items()],
            body=req.content,
        )
        return httpx.Response(
            status_code=result["status"],
            headers=[(k, v) for k, v in result["headers"]],
            content=result["body"],
        )

    return httpx.MockTransport(handler)
