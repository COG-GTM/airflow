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

# Airflow in the browser (WebAssembly)

Airflow 3.3.1 running entirely in a browser tab: the API server, the scheduler, the Dag processor,
the worker and the Postgres metadata database are all WebAssembly, all in the client.  There is no
Airflow process on the server -- the only thing the server does is hand over static files.

```text
page (index.html, src/main.ts)          service worker (public/sw.js)
  boots the runtime worker                intercepts /airflow/*
  ticks the scheduler every 2s            hands each request to the page
  hosts the real Airflow UI in an iframe            |
                     |                              |
                     v                              v
       runtime worker (src/worker.ts) -> Pyodide -> airflow_wasm (python/)
                                                      api.py         ASGI, called directly
                                                      dagparse.py    parses dags/ in-process
                                                      orchestrator.py one cooperative tick
                                                      runner.py      runs tasks in-process
                                                      pglite.py      DBAPI over PGlite
                                                                       |
                                                                       v
                                                              PGlite (Postgres in WASM)
```

## Running it

```bash
npm install     # also copies the Pyodide runtime into public/
npm run dev     # http://localhost:5173/  (first boot downloads ~40 MB of local assets)
npm run build   # typecheck + production bundle into dist/
```

`npm run assets` regenerates the two ignored asset trees: `public/pyodide` (interpreter) and
`public/wheels` (every wheel Airflow needs, resolved once at build time by `scripts/build-wheels.mjs`
so the browser never resolves dependencies against PyPI).

Open the page and the shell reports each boot step; when it says `Airflow ... is up` the iframe below
is the real Airflow UI talking to the in-tab API.  Two demo Dags (`hello_wasm`, `wasm_etl`) are
bundled and start unpaused, so triggering one from the UI runs it in the tab within a couple of ticks.

## How the hard parts work

- **Postgres.**  `pglite.py` is a DBAPI transport: it buffers the Postgres frontend protocol that
  `pg8000` produces and pushes it through PGlite's synchronous `execProtocolRawSync`.  No sockets, no
  `SharedArrayBuffer`, no COOP/COEP headers.  SQLAlchemy uses `StaticPool`, so all access is
  serialized through the one PGlite instance, and Airflow's async connection is turned off (`asyncpg`
  and `greenlet` cannot be built for Emscripten).
- **No processes, no threads.**  `syncasgi.py` drives the ASGI apps to completion synchronously and
  refuses any coroutine that would actually suspend; `patches.py` runs Starlette's threadpool calls
  inline.  Tasks run through the task SDK's in-process supervisor with its socket thread removed, so
  they execute in this interpreter while still talking to the real Execution API.
- **Scheduling.**  `orchestrator.py` is one `tick()`: create scheduled runs, run whatever tasks are
  ready, update run state, and heartbeat the scheduler/triggerer/Dag processor rows the UI reads its
  health badges from.  The page calls it on an interval.
- **Logs.**  `runner.py` points structlog at the same per-attempt log file a real worker would write,
  so the UI's Logs tab reads them locally instead of asking a log server that does not exist.

## Limitations

- **Persistence.**  The metadata database is in-memory by default: PGlite's OPFS-backed datadirs do
  not answer `execProtocolRawSync` once a statement has to touch storage.  A datadir can still be
  asked for with `?dataDir=idb://airflow` and the runtime falls back to memory if it fails, so state
  is lost on reload for now.
- **What can run.**  Anything that stays inside the interpreter: `@task`, PythonOperator, the SDK.
  Not Bash/subprocess operators, virtualenv tasks, Celery/Kubernetes executors, or providers needing
  native libraries or raw TCP.
- **One task at a time.**  Parallelism is 1 and everything is cooperative, so a long task blocks the
  tick that runs it.
- Task logs also contain the in-tab API server's own log lines, because the API really is running in
  the same interpreter as the task.
