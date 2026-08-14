<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Running Airflow on PGlite (experimental)](#running-airflow-on-pglite-experimental)
  - [Reproduction steps](#reproduction-steps)
  - [What works](#what-works)
  - [Limitations](#limitations)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Running Airflow on PGlite (experimental)

[PGlite](https://pglite.dev) is a WASM build of Postgres that normally runs in-process in
Node or the browser, so there is nothing for `psycopg2` to connect to. This directory
runs PGlite behind [`@electric-sql/pglite-socket`](https://github.com/electric-sql/pglite/tree/main/packages/pglite-socket)'s
`pglite-server`, which speaks the Postgres wire protocol over TCP, and Airflow then
uses an ordinary `postgresql+psycopg2://` connection string.

This is a **development/experimental** setup, not a supported backend. See
[Limitations](#limitations): PGlite is a single Postgres backend, so all Airflow
transactions are serialised and a couple of Airflow code paths need adjusting.

Verified with Airflow **3.3.1** on **Python 3.12**, PGlite **0.5.4**
(`PostgreSQL 18.3 on wasm32-unknown-linux-gnu`) and pglite-socket **0.2.7**.

## Reproduction steps

Prerequisites: Node.js >= 20 (for the gateway) and a virtualenv with Airflow and the
Postgres extra installed.

```shell
# 1. Airflow venv (any Airflow >= 3.3 install works; source checkouts work too)
python3 -m venv ~/af-venv
~/af-venv/bin/pip install "apache-airflow[postgres]==3.3.1" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"

# 2. Start PGlite behind the wire-protocol gateway on 127.0.0.1:55432
uv run dev/pglite/pglite_gateway.py start --fresh

# 3. Create the Airflow schema in PGlite
~/af-venv/bin/python dev/pglite/airflow_pglite.py migrate

# 4. Start api-server, scheduler, Dag processor and triggerer
~/af-venv/bin/python dev/pglite/airflow_pglite.py start

# 5. Run a Dag
export AIRFLOW_HOME=~/airflow-pglite-home
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@127.0.0.1:55432/postgres
~/af-venv/bin/airflow dags unpause example_bash_operator
~/af-venv/bin/airflow dags trigger example_bash_operator
```

The UI is on http://localhost:8080; the generated `admin` password is in
`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`.

Inspect the metadata database with any Postgres client, e.g.

```shell
PGPASSWORD=postgres psql -h 127.0.0.1 -p 55432 -U postgres \
  -c "select run_id, state from dag_run"
```

Shut everything down with `airflow_pglite.py stop` and `pglite_gateway.py stop`. The
database lives in `~/.airflow-pglite/data` (override with `PGLITE_ROOT`) and survives
restarts; `--fresh` deletes it.

`uv run dev/pglite/airflow_pglite.py …` also works when the workspace venv has Airflow
installed; the script only needs an interpreter that can `import airflow`.

## What works

- `airflow db migrate` (ORM `create_all` + alembic stamp, and subsequent alembic upgrades)
- api-server (REST API + React UI), scheduler, Dag processor, triggerer, `LocalExecutor`
- Dag parsing and serialisation, Dag runs, task instances, XCom, logs, pools
- `SELECT … FOR UPDATE SKIP LOCKED`, `pg_advisory_lock` / `pg_try_advisory_xact_lock`,
  sequences, `information_schema`, `pg_stat_activity` — the scheduler's locking
  primitives are all present in PGlite 0.5.4 (Postgres 18.3)
- `example_bash_operator` runs to `success` with all task state persisted in PGlite
- In the UI: login, the Dags list, and a Dag run's detail page (state, duration)

The UI is only partly usable — see limitation 6.

## Limitations

PGlite is **one** Postgres backend, i.e. a single session. `pglite-server` therefore
multiplexes all client connections onto it, running one query at a time and pinning the
backend to a single client while that client has a transaction open (`--max-connections`
bounds queued clients, not parallelism). Consequences:

1. **No concurrent transactions.** Whenever an Airflow process holds a transaction open,
   every other process' queries wait. Throughput is far below real Postgres, and only
   a single scheduler is viable.
2. **Two open transactions in one process = permanent deadlock.** If a process holds
   transaction A open on one connection and then waits for a query on a second
   connection, the gateway never gets the backend back and the whole database wedges
   until it is restarted. Airflow's DB init does this twice, which is why
   `airflow db migrate` hangs forever (no error, no timeout) and `airflow_pglite.py migrate`
   is needed:
   - `create_global_lock()` takes a session-level `pg_advisory_lock` on a **second**
     connection and holds it for the whole migration — hangs at
     `Creating global lock context`.
   - `_create_db_from_orm_default()` calls `metadata.create_all(engine)` on a new
     connection while the ORM session still holds the read transaction from
     `_get_current_revision()` — hangs at `Creating metadata`.
   Both locks only protect against *concurrent* migrations, which cannot happen with a
   single-writer PGlite file, so the script drops the cross-connection lock and keeps
   DB access sequential.
   For the same reason `airflow standalone` cannot be used: it runs the unpatched
   `db migrate` at start-up. Start the components individually instead
   (`airflow_pglite.py start`).
3. **Row locks do not isolate anything.** All client connections share one backend
   session, so locks taken by one Airflow component are owned by the same session as
   every other component's. `FOR UPDATE … SKIP LOCKED` never actually skips a *foreign*
   lock; the serialisation of transactions is what keeps schedulers from colliding.
   This is fine for a single scheduler and unsafe for HA setups.
4. **Session state is shared.** `SET`s, temp tables and prepared statements issued by
   one client connection are visible to all of them.
5. **A wedged gateway needs a restart, Airflow first.** Once the single backend is stuck
   in an abandoned transaction, all clients hang. Recover with `airflow_pglite.py stop`,
   then `pglite_gateway.py stop`/`start`, then `airflow_pglite.py start` — restarting only
   the gateway does not help, because Airflow's pooled connections re-wedge it
   immediately. Committed data survives; uncommitted work is lost.
6. **Parts of the UI never load.** The Dag detail page and a run's task-instance table
   issue several API requests in parallel; with a pool of one they queue behind each other
   until SQLAlchemy's pool timeout and return `500 QueuePool … connection timed out`, so
   those views sit on skeleton placeholders indefinitely. Widening the pool to 10/10 was
   measured to be *worse*: the extra connections sit idle-in-transaction and wedge the
   gateway (limitation 5). Use the CLI or `psql` for per-task state. Triggering a Dag from
   the UI is affected too, since the Trigger button lives on that page — use
   `airflow dags trigger`.
7. **Do not run tasks while the api-server is down.** Unrelated to PGlite, but easy to
   hit here: tasks talk to the Execution API, so they fail if the api-server is not up.
