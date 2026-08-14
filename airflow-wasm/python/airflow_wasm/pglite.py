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
Synchronous Postgres connectivity from Python to a PGlite instance living in the same JS realm.

PGlite exposes ``execProtocolRawSync(bytes) -> bytes``, a blocking entry point that consumes
Postgres frontend wire-protocol messages and returns the backend response.  That is enough to run
an unmodified pure-Python driver (``pg8000``) against it: instead of a TCP socket we hand pg8000 a
file-like object whose ``flush()`` ships the buffered frontend messages to PGlite and whose
``read()`` serves the response.

Framing is the important part.  PGlite processes one call as one complete conversation and
terminates it with ``ReadyForQuery``, so an extended-protocol sequence must arrive in a single call.
pg8000 buffers a whole sequence (Parse/Bind/Describe/Execute/Sync) and flushes once, which lines up
exactly; tunnelling the same driver over a TCP transport instead splits sequences across calls and
loses prepared-statement state.
"""

from __future__ import annotations

import io
from typing import Any

_pglite: Any = None

#: PGlite is a single Postgres backend: only one connection may ever be open against it.
DEFAULT_DSN = "postgresql+pg8000://postgres@pglite/postgres"


def set_pglite(instance: Any) -> None:
    """Register the JS-side PGlite instance used by every connection."""
    global _pglite
    _pglite = instance


def get_pglite() -> Any:
    if _pglite is None:
        raise RuntimeError("PGlite instance not registered; call airflow_wasm.pglite.set_pglite()")
    return _pglite


def _to_js_bytes(payload: bytes) -> Any:
    from pyodide.ffi import to_js

    return to_js(payload)


def _from_js_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    to_bytes = getattr(value, "to_bytes", None)
    if to_bytes is not None:
        return to_bytes()
    return bytes(value.to_py())


class PGliteStream(io.RawIOBase):
    """File-like object pg8000 talks to, backed by ``execProtocolRawSync``."""

    def __init__(self) -> None:
        self._out = bytearray()
        self._in = b""
        self._pos = 0
        self._closed = False

    # -- pg8000 uses write/flush/read only ---------------------------------
    def write(self, data: bytes) -> int:  # type: ignore[override]
        self._out.extend(data)
        return len(data)

    def flush(self) -> None:
        if not self._out:
            return
        message = bytes(self._out)
        self._out.clear()
        response = _from_js_bytes(get_pglite().execProtocolRawSync(_to_js_bytes(message)))
        if self._pos and self._pos == len(self._in):
            self._in = response
        else:
            self._in = self._in[self._pos :] + response
        self._pos = 0

    def read(self, size: int = -1) -> bytes:  # type: ignore[override]
        if size is None or size < 0:
            size = len(self._in) - self._pos
        chunk = self._in[self._pos : self._pos + size]
        self._pos += len(chunk)
        return chunk

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True

    def close(self) -> None:
        self._closed = True
        self._out.clear()
        self._in = b""
        self._pos = 0


class _PGliteSocket:
    """Minimal ``socket``-alike; pg8000 only needs ``makefile`` and ``close``."""

    def __init__(self) -> None:
        self._stream = PGliteStream()

    def makefile(self, mode: str = "rwb", *args: Any, **kwargs: Any) -> PGliteStream:
        return self._stream

    def settimeout(self, timeout: float | None) -> None:
        pass

    def setsockopt(self, *args: Any) -> None:
        pass

    def close(self) -> None:
        self._stream.close()


def connect(*args: Any, **kwargs: Any) -> Any:
    """DBAPI ``connect`` used as SQLAlchemy's ``creator``."""
    import pg8000.dbapi

    kwargs.pop("host", None)
    kwargs.pop("port", None)
    kwargs.pop("password", None)
    return pg8000.dbapi.connect(
        user=kwargs.pop("user", "postgres"),
        database=kwargs.pop("database", "postgres"),
        sock=_PGliteSocket(),
        ssl_context=False,
        **kwargs,
    )


def create_engine(url: str = DEFAULT_DSN, **engine_args: Any) -> Any:
    """Create a SQLAlchemy engine bound to the single PGlite backend."""
    from sqlalchemy import create_engine as sa_create_engine
    from sqlalchemy.pool import StaticPool

    for pool_arg in ("pool_size", "max_overflow", "pool_recycle", "pool_pre_ping", "poolclass"):
        engine_args.pop(pool_arg, None)
    return sa_create_engine(
        url,
        creator=connect,
        poolclass=StaticPool,
        **engine_args,
    )
