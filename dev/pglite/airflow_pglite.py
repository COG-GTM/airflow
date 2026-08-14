#!/usr/bin/env python3
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
#
# /// script
# requires-python = ">=3.10"
# ///
"""
Run Airflow with a PGlite metadata database (see dev/pglite/README.md).

``migrate`` performs the schema creation/migration with two PGlite-specific
adjustments, ``start``/``stop`` manage the api-server, scheduler, Dag processor
and triggerer, and ``env`` prints the environment the components need.

Must be executed with a Python interpreter that has Airflow installed, e.g.
``uv run --project airflow-core dev/pglite/airflow_pglite.py migrate`` or
``/path/to/airflow-venv/bin/python dev/pglite/airflow_pglite.py migrate``.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import subprocess
import sys
import time
from pathlib import Path

HOST = os.environ.get("PGLITE_HOST", "127.0.0.1")
PORT = os.environ.get("PGLITE_PORT", "55432")
AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", Path.home() / "airflow-pglite-home"))

ENVIRONMENT = {
    "AIRFLOW_HOME": str(AIRFLOW_HOME),
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"postgresql+psycopg2://postgres:postgres@{HOST}:{PORT}/postgres",
    # PGlite runs one statement at a time, so extra pooled connections only add
    # transactions competing for the single backend. Widening the pool to 10/10 was
    # measured to be worse: more connections sitting idle-in-transaction starve the
    # backend and wedge the gateway outright.
    "AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE": "1",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW": "1",
    "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
    "AIRFLOW__CORE__LOAD_EXAMPLES": "True",
    "AIRFLOW__CORE__PARALLELISM": "4",
    "AIRFLOW__SCHEDULER__PARSING_PROCESSES": "1",
    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": "admin:admin",
}

COMPONENTS = (
    (["api-server", "--port", os.environ.get("AIRFLOW_API_PORT", "8080")], 20),
    (["scheduler"], 10),
    (["dag-processor"], 5),
    (["triggerer"], 5),
)


def build_env() -> dict[str, str]:
    env = dict(os.environ)
    for key, value in ENVIRONMENT.items():
        env.setdefault(key, value)
    return env


def migrate() -> int:
    """
    Create/upgrade the Airflow schema on PGlite.

    PGlite is a single Postgres backend, so the gateway can only ever have one
    transaction open at a time: a second connection that wants to run a statement
    waits until the open transaction commits. Airflow's DB init path breaks that
    invariant twice, and each one wedges the gateway indefinitely:

    1. ``create_global_lock()`` takes a session-level ``pg_advisory_lock`` on a
       *second* connection and holds it for the whole migration.
    2. ``_create_db_from_orm_default()`` calls ``metadata.create_all(engine)`` on a
       new connection while the ORM session still holds the read transaction
       opened by ``_get_current_revision()``.

    Both only guard against concurrent migrations of the same database, which
    cannot happen with a single-writer PGlite file, so we drop the cross-connection
    lock and keep the DB access strictly sequential instead.
    """
    os.environ.update({k: v for k, v in build_env().items() if k not in os.environ})
    AIRFLOW_HOME.mkdir(parents=True, exist_ok=True)

    from alembic import command

    import airflow.utils.db as airflow_db
    from airflow.models.base import Base

    @contextlib.contextmanager
    def no_global_lock(*_args, **_kwargs):
        yield

    def create_db_from_orm_sequential(session) -> None:
        session.rollback()
        Base.metadata.create_all(session.get_bind().engine)
        command.stamp(airflow_db._get_alembic_config(), "head")

    airflow_db.create_global_lock = no_global_lock
    airflow_db._create_db_from_orm_default = create_db_from_orm_sequential

    airflow_db.upgradedb()
    print("Airflow schema created/migrated on PGlite")
    return 0


def start() -> int:
    env = build_env()
    airflow_bin = Path(sys.executable).with_name("airflow")
    AIRFLOW_HOME.mkdir(parents=True, exist_ok=True)
    for args, settle_seconds in COMPONENTS:
        log_path = AIRFLOW_HOME / f"{args[0]}.log"
        print(f"Starting airflow {args[0]} (log: {log_path})")
        with log_path.open("ab") as log:
            subprocess.Popen(
                [str(airflow_bin), *args],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        # Components are staggered so that they do not contend for the single
        # PGlite backend while each one runs its start-up queries.
        time.sleep(settle_seconds)
    print(f"Airflow started. UI: http://localhost:{env.get('AIRFLOW_API_PORT', '8080')}")
    return 0


def stop() -> int:
    # api-server/worker processes rewrite their process title, hence the extra patterns.
    patterns = (
        "airflow api-server",
        "airflow api_server",
        "airflow scheduler",
        "airflow dag-processor",
        "airflow triggerer",
        "airflow serve-logs",
        "airflow worker",
    )
    for pattern in patterns:
        subprocess.run(["pkill", "-f", pattern], check=False)
    time.sleep(3)
    print("Airflow components stopped")
    return 0


def print_env() -> int:
    for key, value in build_env().items():
        if key in ENVIRONMENT:
            print(f"export {key}={value!r}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["migrate", "start", "stop", "env"])
    args = parser.parse_args()
    return {"migrate": migrate, "start": start, "stop": stop, "env": print_env}[args.command]()


if __name__ == "__main__":
    sys.exit(main())
