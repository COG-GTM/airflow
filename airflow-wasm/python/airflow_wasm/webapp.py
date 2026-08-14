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
The surface the browser worker calls: boot, serve a request, run a scheduler tick.

Everything here returns plain JS values so ``worker.ts`` never has to touch a ``PyProxy``.
"""

from __future__ import annotations

from typing import Any


def boot(instance: Any, *, base_url: str, dags: Any = None) -> dict[str, Any]:
    """Configure Airflow against ``instance`` (a PGlite database), migrate it, and load Dag files."""
    from airflow_wasm import bootstrap

    # ``base_url`` is where the UI is mounted in the tab; Airflow turns its path into the app's
    # ``root_path``, which is what makes the SPA ask for ``/airflow/api/v2/...``.
    bootstrap.configure(instance, env={"AIRFLOW__API__BASE_URL": base_url})
    bootstrap.migrate()

    from airflow_wasm import dagparse

    written = []
    for filename, source in dict(dags or {}).items():
        written.append({"file": filename, **dagparse.write_dag(filename, source)})

    from airflow.configuration import conf

    return {
        "airflow_version": _version(),
        "base_url": conf.get("api", "base_url"),
        "dags": written,
    }


def _version() -> str:
    from airflow import __version__

    return __version__


async def http(
    method: str,
    url: str,
    headers: Any,
    body: Any,
) -> Any:
    """
    Serve one request from the UI out of the in-browser Airflow app.

    ``headers`` is a sequence of name/value pairs and ``body`` a buffer of request bytes, both as
    handed over by the worker.
    """
    from airflow_wasm import api

    header_pairs = [(str(name), str(value)) for name, value in dict(headers).items()]
    response = await api.handle(method, url, header_pairs, bytes(body))
    return _to_js(
        {
            "status": response["status"],
            "headers": [[_text(name), _text(value)] for name, value in response["headers"]],
            "body": response["body"],
        }
    )


def _text(value: str | bytes) -> str:
    """ASGI header names and values may reach us as either bytes or text."""
    return value.decode("latin-1") if isinstance(value, bytes) else value


def tick() -> Any:
    """Run one scheduler/executor step and report what happened."""
    from airflow_wasm import orchestrator

    return _to_js(orchestrator.tick())


def write_dag(filename: str, source: str) -> Any:
    from airflow_wasm import dagparse

    return _to_js(dagparse.write_dag(filename, source))


def _to_js(value: Any) -> Any:
    import js
    from pyodide.ffi import to_js

    return to_js(value, dict_converter=js.Object.fromEntries)
