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
Serve Airflow's real FastAPI application without a socket.

There is no HTTP server in the tab: the service worker intercepts the UI's ``fetch`` calls and the
worker replays them straight into the ASGI app as ``http`` scope dictionaries.  Speaking ASGI
directly (rather than through an HTTP client shim) keeps the whole request cycle in one coroutine and
avoids needing lifespan-managed sockets, uvicorn or h11.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlsplit

_app: Any = None


def get_app() -> Any:
    """Build (once) Airflow's FastAPI app, including the UI and both API surfaces."""
    global _app
    if _app is None:
        from airflow.api_fastapi.app import create_app

        _app = create_app(apps="all")
    return _app


async def handle(
    method: str,
    url: str,
    headers: list[tuple[str, str]] | None = None,
    body: bytes = b"",
) -> dict[str, Any]:
    """Run one request through the ASGI app and collect the whole response."""
    app = get_app()
    split = urlsplit(url)
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method.upper(),
        "scheme": split.scheme or "http",
        # ASGI servers hand over a percent-decoded path (with the raw one alongside it); path
        # parameters such as a run id of manual__2026-01-01T00:00:00+00:00 depend on it.
        "path": unquote(split.path or "/"),
        "raw_path": (split.path or "/").encode(),
        "query_string": split.query.encode(),
        "root_path": "",
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or [])],
        "client": ("127.0.0.1", 0),
        "server": (split.hostname or "localhost", split.port or 80),
        "state": {},
    }

    request_sent = False

    async def receive() -> dict[str, Any]:
        nonlocal request_sent
        if request_sent:
            return {"type": "http.disconnect"}
        request_sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    status = 500
    response_headers: list[tuple[str, str]] = []
    chunks = bytearray()

    async def send(message: dict[str, Any]) -> None:
        nonlocal status
        if message["type"] == "http.response.start":
            status = message["status"]
            response_headers.extend(
                (k.decode("latin-1"), v.decode("latin-1")) for k, v in message.get("headers", [])
            )
        elif message["type"] == "http.response.body":
            chunks.extend(message.get("body", b""))

    await app(scope, receive, send)
    return {"status": status, "headers": response_headers, "body": bytes(chunks)}
