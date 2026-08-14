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
Run task instances in this interpreter, still talking to the real Execution API.

Airflow normally supervises a task in a forked child that reaches the Execution API over HTTP.  The
task SDK already has an in-process variant of that (``InProcessTestSupervisor``, used by
``dag.test()``), but it needs two things Pyodide cannot give it: a background thread to service the
supervisor socket, and ``a2wsgi`` to call the async API from sync code.  Both are replaceable --
the socket is only used by operators that re-exec Python (virtualenv, ``run_as_user``), and the API
can be driven synchronously (see :mod:`airflow_wasm.syncasgi`).

Because the API is the real DB-backed one, task state, XComs and logs land in the metadata database
exactly as they would with a real worker, so the UI shows genuine run history.
"""

from __future__ import annotations

import contextlib
import sys
from typing import Any

from airflow_wasm import syncasgi

_app: Any = None
_client: Any = None
_dags: dict[tuple[str, str], tuple[float, Any]] = {}


def execution_api_app() -> Any:
    """Build the Execution API app (once) with auth bypassed, and start its lifespan."""
    global _app
    if _app is None:
        from airflow.sdk.execution_time.supervisor import in_process_api_server

        app = in_process_api_server().app
        syncasgi.make_synchronous(app)
        # The app is never served, so nothing else runs its lifespan; svcs needs it for its registry.
        _lifespan = app.router.lifespan_context(app)
        syncasgi.drive(_lifespan.__aenter__())
        _app = app
    return _app


def client() -> Any:
    """Build (once) an Execution API client that dispatches straight into the ASGI app."""
    global _client
    if _client is None:
        from airflow.sdk.execution_time.supervisor import InProcessTestSupervisor

        _client = InProcessTestSupervisor._Client(
            base_url=None,
            token="",
            dry_run=True,
            transport=syncasgi.httpx_transport(execution_api_app()),
        )
        _client.base_url = "http://in-process.invalid./"
    return _client


def supervisor_class() -> Any:
    """``InProcessTestSupervisor`` minus its background socket thread."""
    from airflow.sdk.execution_time.supervisor import InProcessTestSupervisor

    class WasmSupervisor(InProcessTestSupervisor):
        @contextlib.contextmanager
        def _setup_subprocess_socket(self) -> Any:
            # Only operators that re-exec Python (virtualenv, run_as_user) use this socket, and those
            # cannot run in the browser anyway.  Creating it would need a thread to service it.
            yield None

    return WasmSupervisor


def sdk_task(dag_id: str, task_id: str, bundle_name: str, rel_path: str) -> Any:
    """
    Import the DAG file and return the live task object.

    The task cannot come from the metadata DB: a serialized operator carries the scheduler's view of a
    task (dependencies, defaults) but not its ``execute()``.  A real worker re-imports the DAG file for
    exactly this reason; here the import is just cheaper, and cached until the file changes.
    """
    import os

    from airflow.dag_processing.bundles.manager import DagBundlesManager
    from airflow.dag_processing.dagbag import BundleDagBag

    bundle = DagBundlesManager().get_bundle(name=bundle_name)
    bundle.initialize()
    path = os.path.join(bundle.path, rel_path)

    key = (bundle_name, rel_path)
    mtime = os.path.getmtime(path)
    cached = _dags.get(key)
    if cached is None or cached[0] != mtime:
        bag = BundleDagBag(
            dag_folder=path,
            safe_mode=False,
            load_op_links=False,
            bundle_path=bundle.path,
            bundle_name=bundle_name,
        )
        _dags[key] = (mtime, bag)
        cached = _dags[key]

    return cached[1].dags[dag_id].task_dict[task_id]


class _LogSink:
    """
    Byte sink that follows whichever task is currently running.

    Loggers built while a task runs are cached by ``structlog`` and outlive it, so they must not hold
    the log file itself -- they hold this, and it forwards to the console once the file is gone.
    """

    def __init__(self) -> None:
        self.file: Any = None

    def write(self, data: bytes) -> None:
        if self.file is not None:
            self.file.write(data)
        else:
            sys.stderr.write(data.decode("utf-8", "replace"))

    def flush(self) -> None:
        if self.file is not None:
            self.file.flush()


_SINK = _LogSink()


@contextlib.contextmanager
def task_log_file(log_path: str) -> Any:
    """
    Send this task's logs to the file the UI reads them from.

    A real worker has its log file wired up by the supervisor before it execs the child; the
    in-process supervisor logs through whatever ``structlog`` is globally configured with, so the
    global configuration is what has to be swapped -- writing JSON lines, which is the format the
    task log API parses back out.
    """
    import structlog

    from airflow.sdk.log import init_log_file, logging_processors

    _SINK.file = init_log_file(log_path).open("ab")
    previous = structlog.get_config()
    structlog.configure(
        processors=list(logging_processors(json_output=True)),
        logger_factory=structlog.BytesLoggerFactory(file=_SINK),
    )
    try:
        yield
    finally:
        structlog.configure(**previous)
        _SINK.file.close()
        _SINK.file = None


def run_task(ti: Any, *, bundle_name: str, rel_path: str, log_path: str | None = None) -> Any:
    """Run one task instance to completion; returns a ``TaskRunResult``."""
    task = sdk_task(ti.dag_id, ti.task_id, bundle_name, rel_path)
    with contextlib.ExitStack() as stack:
        if log_path:
            stack.enter_context(task_log_file(log_path))
        return supervisor_class().start(what=ti, task=task, client=client())
