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
Turn a bare Pyodide interpreter into a configured Airflow installation backed by PGlite.

Two hooks do the work.  Airflow imports ``airflow_local_settings`` if it is importable and copies
public names over ``airflow.settings``, so a synthetic module registered in ``sys.modules`` can
replace ``create_metadata_engine`` with one that hands SQLAlchemy the PGlite connection and a
``StaticPool`` -- PGlite is a single Postgres backend, so every session must share one connection.
Leaving ``sql_alchemy_conn_async`` empty makes Airflow skip the async engine entirely, which is what
we want since ``asyncpg`` needs sockets and SQLAlchemy's async bridge needs greenlet.
"""

from __future__ import annotations

import os
import sys
from types import ModuleType
from typing import Any

from airflow_wasm import pglite

AIRFLOW_HOME = "/airflow"

#: Config that must be in place before ``airflow.configuration`` is first imported.
ENV = {
    "AIRFLOW_HOME": AIRFLOW_HOME,
    "AIRFLOW__CORE__DAGS_FOLDER": f"{AIRFLOW_HOME}/dags",
    "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    # The bundled demo Dags should be runnable the moment the tab finishes booting.
    "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "False",
    "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
    "AIRFLOW__CORE__PARALLELISM": "1",
    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": "True",
    "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY": "airflow-wasm",
    "AIRFLOW__API__SECRET_KEY": "airflow-wasm",
    "AIRFLOW__API__EXPOSE_CONFIG": "True",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": pglite.DEFAULT_DSN,
    # Empty on purpose: disables Airflow's async engine (see module docstring).
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC": "",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_ENABLED": "False",
    "AIRFLOW__DATABASE__CHECK_MIGRATIONS": "False",
    "AIRFLOW__LOGGING__BASE_LOG_FOLDER": f"{AIRFLOW_HOME}/logs",
    "AIRFLOW__LOGGING__COLORED_CONSOLE_LOG": "False",
    "AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR": "False",
    "AIRFLOW__SCHEDULER__PARSING_PROCESSES": "1",
    "AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL": "0",
    "AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK": "False",
    "AIRFLOW__TRACES__OTEL_ON": "False",
    "AIRFLOW__METRICS__OTEL_ON": "False",
    # In-browser task runs happen in this very interpreter; no supervisor heartbeat exists.
    "AIRFLOW__SDK__EXECUTION_API_SERVER_URL": "http://in-process/execution/",
}


def _local_settings_module() -> ModuleType:
    module = ModuleType("airflow_local_settings")
    # Airflow logs ``__file__`` after importing local settings.
    module.__file__ = "<airflow_wasm.bootstrap>"

    def create_metadata_engine(
        sql_alchemy_conn: str,
        *,
        engine_args: dict[str, Any],
        connect_args: dict[str, Any],
    ) -> Any:
        return pglite.create_engine(sql_alchemy_conn)

    module.create_metadata_engine = create_metadata_engine  # type: ignore[attr-defined]
    module.__all__ = ["create_metadata_engine"]  # type: ignore[attr-defined]
    return module


def configure(instance: Any, *, env: dict[str, str] | None = None) -> None:
    """Point Airflow at ``instance`` (a JS PGlite object).  Must run before importing airflow."""
    if "airflow.configuration" in sys.modules:
        raise RuntimeError("airflow_wasm.bootstrap.configure() must run before importing airflow")

    pglite.set_pglite(instance)
    os.environ.update(ENV)
    if env:
        os.environ.update(env)
    for path in (AIRFLOW_HOME, f"{AIRFLOW_HOME}/dags", f"{AIRFLOW_HOME}/logs"):
        os.makedirs(path, exist_ok=True)
    sys.modules.setdefault("airflow_local_settings", _local_settings_module())


def patch() -> None:
    """Apply the runtime patches Airflow needs against PGlite (safe to call once, after import)."""
    from airflow_wasm import patches

    patches.apply()


def migrate() -> None:
    """Create the Airflow metadata schema in PGlite by running its Alembic migrations."""
    patch()

    from airflow.utils import db

    db.initdb()


def session() -> Any:
    from airflow.utils.session import create_session

    return create_session()
