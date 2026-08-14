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
"""Runtime patches Airflow needs when its database is a single-backend PGlite instance."""

from __future__ import annotations

import contextlib
import sys
from typing import Any


def _patch_global_lock() -> None:
    """
    Make ``create_global_lock`` a no-op.

    It exists to stop concurrent Airflow processes migrating the same database, and it implements
    that by opening a second connection and passing the timeout as a bind parameter -- neither of
    which works here.  PGlite is a single Postgres backend shared through a ``StaticPool``, so the
    "second" connection is the same one the migration is running on, and ``SET LOCK_TIMEOUT to $1``
    is not valid Postgres (psycopg2 hides that by interpolating client-side).  A browser tab runs
    exactly one Airflow, so there is nothing to lock against.
    """
    from airflow.utils import db

    @contextlib.contextmanager
    def create_global_lock(session: Any, lock: Any, lock_timeout: int = 1800) -> Any:
        yield

    db.create_global_lock = create_global_lock


def _patch_threadpool() -> None:
    """
    Run "blocking" work inline instead of in a worker thread.

    Pyodide's default build has no pthreads, so ``threading.Thread.start()`` raises.  Starlette hands
    every ``def`` (non-``async def``) endpoint to ``anyio.to_thread.run_sync``, which would make all
    of Airflow's sync routes fail.  Running them directly on the event loop is safe here because the
    whole runtime is single-threaded anyway and PGlite blocks the loop regardless.
    """
    import anyio.to_thread

    async def run_sync(func: Any, *args: Any, **kwargs: Any) -> Any:
        return func(*args)

    anyio.to_thread.run_sync = run_sync

    import starlette.concurrency

    async def run_in_threadpool(func: Any, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    starlette.concurrency.run_in_threadpool = run_in_threadpool
    for module_name in ("starlette.routing", "starlette.responses", "fastapi.routing"):
        module = sys.modules.get(module_name)
        if module is not None and hasattr(module, "run_in_threadpool"):
            module.run_in_threadpool = run_in_threadpool


def _patch_dag_import_timeout() -> None:
    """
    Drop the ``dagbag_import_timeout`` alarm around DAG imports.

    The importer arms it with ``signal.setitimer``, which Emscripten does not implement (there is no
    preemption to deliver the alarm with).  Without the patch every parse fails with an
    ``AttributeError`` before the DAG file is even read.  A runaway DAG file will hang the tab
    instead of being interrupted -- unavoidable in a single-threaded runtime.
    """
    from airflow.dag_processing.importers import python_importer

    @contextlib.contextmanager
    def _timeout(seconds: int, error_message: str | None = None) -> Any:
        yield

    python_importer._timeout = _timeout


def apply() -> None:
    _patch_threadpool()
    _patch_dag_import_timeout()
    _patch_global_lock()
