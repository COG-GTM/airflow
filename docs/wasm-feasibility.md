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

# Running Airflow in WebAssembly — feasibility study

Status: **feasibility report, not a proposal to merge runtime changes.** Nothing in this document
changes Airflow behaviour; it records an audit of the source tree in this repository (commit
`39ad570`, `airflow-core` version `3.4.0`) plus an empirical spike run against the published
`apache-airflow-core==3.3.1` wheels under Pyodide.

Every claim below is tagged:

- **[EVIDENCE]** — observed output from the spike, or a literal line in this repository.
- **[INFERENCE]** — reasoned from evidence/upstream docs, not directly executed here.

## 0. Headline result

A two-task Dag (`PythonOperator`-style `@task` functions, with XCom passing) **ran to success
entirely inside WebAssembly** — CPython 3.14 on `wasm32-emscripten` (Pyodide), SQLite metadata
database in the WASM filesystem, Airflow's FastAPI Execution API served in-process over ASGI, no
host Python, no Postgres, no container, no subprocess, no thread. **[EVIDENCE]**

```text
patched InProcessExecutionAPI.transport OK
patched anyio worker-thread offload -> inline call
patched InProcessTestSupervisor._setup_subprocess_socket -> no-op (no socketpair, no thread)
   lifespan startup -> [{'type': 'lifespan.startup.complete'}]
hello from wasm
   dagrun state: success
   TI hello success
   TI double success
   XCOM hello return_value 42
   XCOM double return_value 84
```

That result needed exactly five deviations from stock Airflow (§1.3). None of them is a dead end:
four map to small, upstreamable changes, and the fifth (four missing C-extension wheels) is a
packaging problem, not an architectural one.

The honest verdict (§9): **"WASM Airflow" as *the whole product* — scheduler + triggerer +
DagFileProcessor + LocalExecutor + Postgres + gunicorn — is blocked and will stay blocked.** But a
**single-tenant, cooperative, in-process Airflow profile** — parse Dags, run Python tasks, persist
real metadata, serve the real REST API and the real React UI — is *not* blocked today. It is
achievable, and the pieces it needs already exist in the tree for testing purposes
(`InProcessExecutionAPI`, `InProcessTestSupervisor`, `DAG.test()`, `dagbag`-level parsing).

## 1. The spike (read this before the analysis)

### 1.1 Setup

| Item | Value |
| --- | --- |
| Runtime | Pyodide (npm `pyodide`), reported build `314.0.3`, CPython **3.14.2**, `wasm32-emscripten` |
| Host | Node 20.20.2, `node --experimental-wasm-stack-switching` |
| Airflow | `apache-airflow-core==3.3.1` + `apache-airflow-task-sdk==1.3.1` from PyPI via `micropip` |
| Metadata DB | `sqlite:////airflow/airflow.db` inside the Emscripten MEMFS |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `False` |

The published 3.3.1 wheels were used rather than this checkout's `3.4.0` sources because `micropip`
resolves from PyPI; the 3.4.0 dependency list in `airflow-core/pyproject.toml` differs only in
version floors, so the audit transfers. **[INFERENCE]**

### 1.2 What worked, unmodified

- `micropip.install("apache-airflow-core")` resolved a **113-package** dependency closure. **[EVIDENCE]**
- `airflow.utils.db.resetdb()` created the full metadata schema on SQLite (all Alembic migrations
  ran). **[EVIDENCE]**
- `DagBag(dag_folder=...)` parsed a real Dag file in-process. **[EVIDENCE]**
- The Execution API FastAPI app was constructed, its lifespan ran, and task-instance
  state transitions went through it (`Task started`, `Task instance state updated` from
  `airflow.api_fastapi.execution_api.routes.task_instances`). **[EVIDENCE]**
- `pendulum`, `cryptography`, `pydantic-core`, `msgspec`, `libcst`, `protobuf`, `rpds-py`, `wrapt`,
  `PyYAML` — all normally native — installed fine (§2). **[EVIDENCE]**

### 1.3 The five deviations required

| # | Deviation | Why it was needed | Upstreamable fix |
| --- | --- | --- | --- |
| 1 | Local no-op stub wheels for `psutil`, `setproctitle`, `greenlet`, `grpcio` | no pure-Python wheels; `micropip` cannot build C extensions | make these optional / provide WASM builds (§2.2) |
| 2 | `signal.setitimer = lambda *a: (0.0, 0.0)` | absent on Emscripten; used by the Dag import timeout | timeout backend that degrades instead of raising (§3.4) |
| 3 | Replaced `InProcessExecutionAPI.transport` with a JSPI-backed sync→async ASGI transport, and ran the ASGI `lifespan` manually | stock `.transport` starts a background thread (`threading.Thread(target=loop.run_forever)`); threads are unavailable | thread-free sync transport when no threading (§3.3) |
| 4 | `anyio` worker-thread offload patched to call inline | FastAPI runs *sync* dependencies/endpoints in a threadpool | anyio/starlette-level concern; a "no-thread" anyio backend (§3.3) |
| 5 | `InProcessTestSupervisor._setup_subprocess_socket()` → no-op | it unconditionally creates a `socketpair()` + comms thread, needed only if the task later shells out (VirtualEnv / `run_as_user`) | make the preemptive socketpair lazy (§3.2) |

Deviation 3 is the interesting one. Pyodide's `WebLoop.run_until_complete` needs **JSPI**
(WebAssembly stack switching) to block a synchronous Python frame on a JS promise. Without it:

```text
loop policy: WebLoopPolicy
run_until_complete FAIL: RuntimeError WebAssembly stack switching not supported in this JavaScript runtime
asyncio.run FAIL: RuntimeError WebAssembly stack switching not supported in this JavaScript runtime
JSPI can_run_sync: False
```

With `node --experimental-wasm-stack-switching`: **[EVIDENCE]**

```text
run_until_complete -> 7
asyncio.run -> 7
JSPI can_run_sync: True
```

This single capability is what makes the whole thing tractable: Airflow's task path is deeply
synchronous, the browser is deeply asynchronous, and JSPI is the bridge. JSPI ships enabled in
Chromium ≥ 137; older Chromium needs `--enable-experimental-webassembly-jspi`, and Node needs the
V8 flag above. Firefox/Safari support is not established here. **[INFERENCE]** The Chrome for
Testing binary on this machine is 133, so the browser leg was **not** verified — Node with the V8
flag was the substitute. Treat "runs in a browser tab" as *inferred*, and "runs in WASM with a
JSPI-capable engine" as *evidenced*.

### 1.4 What still failed (before the shims), verbatim

| Attempt | Result **[EVIDENCE]** |
| --- | --- |
| `os.fork()` | `OSError [Errno 52] Function not implemented` |
| `socket.socketpair()` | `OSError [Errno 28] Invalid argument` |
| `subprocess.run([...])` | `OSError [Errno 138] emscripten does not support processes` |
| `threading.Thread(...).start()` | `RuntimeError: can't start new thread` |
| `signal.setitimer` | `AttributeError: module 'signal' has no attribute 'setitimer'` |
| `LocalExecutor().start()` | fails constructing `multiprocessing` queues (`_multiprocessing` missing) |
| `DAG.test()` via stock `InProcessExecutionAPI.transport` | `RuntimeError: can't start new thread` |

Those five OS-level results are the *entire* substance of §3. Everything Airflow does that breaks
in WASM reduces to one of them.

## 2. Dependency audit

### 2.1 The closure that installed

113 packages resolved; **109 installed as-is**. Sources: `pyodide` = prebuilt in the Pyodide
distribution (includes non-trivial native code, cross-compiled by the Pyodide project); `pypi` =
pure-Python wheel (`py3-none-any`) fetched straight from PyPI. **[EVIDENCE]**

Notable "should have been a problem, wasn't":

| Package | Version | Source | Note |
| --- | --- | --- | --- |
| `cryptography` | 47.0.0 | pyodide | Rust + OpenSSL, cross-compiled upstream. Required by `airflow-core` (`cryptography>=44.0.3`) for Fernet |
| `pydantic-core` | 2.41.5 | pyodide | Rust core of `pydantic` |
| `msgspec` | 0.20.0 | pyodide | C; used by shared logging |
| `libcst` | 1.8.6 | pyodide | Rust parser |
| `protobuf` / `rpds-py` / `wrapt` / `PyYAML` / `cffi` | — | pyodide | native, prebuilt |
| **`pendulum`** | **3.2.0** | **pypi** | pendulum 3 ships a pure-Python wheel; the Rust speedup is optional. Required by both `airflow-core` and `task-sdk` |
| `sqlalchemy` | 2.0.52 | pypi | pure-Python wheel installs; C speedups optional |
| `fastapi` / `starlette` / `cadwyn` / `uvicorn` | — | pypi | all pure Python; `uvicorn` *imports* fine (it just can't `listen()`) |
| `alembic`, `croniter`, `cron-descriptor`, `python-daemon`, `structlog`, `svcs`, `a2wsgi`, `aiosqlite`, OTel `*` | — | pypi | pure Python |

`lxml` is **not** in the core closure at all — it enters only via some providers, so it is a
provider-selection problem, not a core blocker. **[EVIDENCE]** Likewise `psycopg2` is absent:
`airflow-core` does not depend on a Postgres driver; `airflow/settings.py` only maps a driver name
when the connection string asks for one.

### 2.2 The four that did not install

| Package | Required by | Class | Path forward |
| --- | --- | --- | --- |
| `psutil` | `airflow-core` (`psutil>=5.8.0`), `task-sdk` (`psutil>=6.1.0`) — used in `utils/process_utils.py`, `cli/hot_reload.py`, `sdk/execution_time/supervisor.py`, `sdk/coordinators/_subprocess.py` | **needs WASM build / pure-Python shim** | every use is about *other processes*; in a no-subprocess profile the calls are vestigial (`psutil.Process()` for "me") |
| `setproctitle` | `airflow-core` (`setproctitle>=1.3.3`) — `executors/local_executor.py`, `executors/base_executor.py`, `cli/commands/api_server_command.py`, `api_fastapi/gunicorn_config.py`, `utils/serve_logs/core.py`, `sdk/execution_time/task_runner.py` | **pure-Python fallback (no-op)** | cosmetic; make the import optional |
| `greenlet` | transitively via `sqlalchemy[asyncio]`; also `greenback` in `task-sdk` | **blocker for async SQLAlchemy only** | stack switching cannot be emulated in `wasm32-emscripten`; use *sync* SQLAlchemy. A `getcurrent()`-only stub was enough to import SQLAlchemy; creating a greenlet must still raise |
| `grpcio` | transitively via `opentelemetry-exporter-otlp` | **avoidable** | drop the gRPC exporter (keep `opentelemetry-exporter-otlp-proto-http`) or ship a no-op |

`greenlet` deserves emphasis: SQLAlchemy's **asyncio** layer is implemented by greenlet stack
switching, so `sqlalchemy[asyncio]`, `aiosqlite`, and `greenback`-based async task paths are the one
genuinely *unemulatable* dependency here. **[EVIDENCE for import failure; INFERENCE for
"unemulatable"]** The escape hatch is that Airflow's core ORM usage is synchronous — the async
engine matters for the triggerer and some API paths, which a WASM profile can decline to run.

### 2.3 Classification summary

| Class | Count | Examples |
| --- | --- | --- |
| Works today in WASM | 109 of 113 | everything in §2.1 |
| Pure-Python fallback needed | 1 | `setproctitle` |
| Needs a WASM build (or is vestigial in-profile) | 2 | `psutil`, `grpcio` |
| Genuine blocker (feature-scoped) | 1 | `greenlet` → async SQLAlchemy / triggerer |

The dependency tree is **not** the hard part. This is the most surprising finding of the study.

## 3. Process model — what actually breaks

WASM (both Emscripten and WASI) has no `fork`, no `exec`, no POSIX signal delivery, and — absent
`SharedArrayBuffer` + pthread builds — no threads. Airflow's runtime is built on all four.

### 3.1 Multiprocessing: `LocalExecutor` and the Dag processor

- `airflow-core/src/airflow/executors/local_executor.py` builds `multiprocessing` queues and worker
  processes. Pyodide has no `_multiprocessing`, so `start()` fails at construction. **[EVIDENCE]**
- `airflow-core/src/airflow/dag_processing/manager.py` runs parsing in parallel child processes;
  `dag_processing/processor.py`'s `DagFileProcessorProcess` is a subprocess wrapper.
- Other `multiprocessing` importers: `jobs/scheduler_job_runner.py`, `settings.py`,
  `utils/sqlalchemy.py`, `utils/process_utils.py`, `cli/commands/{scheduler,triggerer}_command.py`,
  `sdk/bases/operator.py`, `sdk/execution_time/cache.py`.

Replacement: an **in-process executor** that runs the task in the same interpreter (the tree already
has the shape of this — `InProcessTestSupervisor` + `DAG.test()`, whose signature defaults to
`use_executor: bool = False`), and **in-process parsing** via `DagBag`, which the spike exercised.

### 3.2 fork/exec + socketpair: the supervisor

`task-sdk/src/airflow/sdk/execution_time/supervisor.py` is the sharpest edge: `os.fork()` (line
~720), `os.execv(...)` (~761), `os._exit(...)` (~443, ~784), a `socketpair()` comms channel with
inherited fixed FDs, `os.dup2`, and `signal.signal(...)` resets in the child. `task_runner.py` also
has `os.execvp("sudo", cmd)` for `run_as_user`. All of these are unavailable. **[EVIDENCE]**

The in-process variant already exists and *almost* works: the only WASM-fatal line on that path is
the preemptive `socketpair()` in `InProcessTestSupervisor._setup_subprocess_socket()`
(supervisor.py:2077) plus its comms `threading.Thread` (2075) — created "in case the task process
runs VirtualEnv operator or run_as_user". Making it lazy (only when a task actually needs a child)
removes the last obstacle to running tasks. **[EVIDENCE — no-op'ing it produced the successful run
in §0]**

### 3.3 Threads: ASGI transport and FastAPI's sync path

Two thread users sit directly on the task path:

1. `airflow-core/src/airflow/api_fastapi/execution_api/app.py` — `InProcessExecutionAPI.transport`
   spins `threading.Thread(target=loop.run_forever)` and wraps the ASGI app with `a2wsgi`'s
   `ASGIMiddleware` to expose a *sync* httpx transport. `.atransport` (plain
   `httpx.ASGITransport`) needs no thread. `jobs/triggerer_job_runner.py` uses the same class.
2. `anyio.to_thread.run_sync`, reached from `starlette.concurrency.run_in_threadpool`, is used by
   FastAPI for **sync** dependencies and endpoints — so even a fully async caller hits a thread.

The JSPI transport in §1.3 replaces (1) with ~20 lines and no thread; (2) needs a
"run-inline" anyio offload. Both are single-process concerns with no semantic change when there is
exactly one task in flight. **[INFERENCE, backed by the passing run]**

### 3.4 Signals and timers

23 modules touch `signal.*`. Two categories:

- **Process control** — `SIGTERM`/`SIGINT`/`SIGUSR2` handling in `supervisor.py`,
  `local_executor.py`, `base_executor.py`, `daemon_utils.py`, `gunicorn_app.py`,
  `cli/hot_reload.py`. Meaningless with no children; can be skipped.
- **Timeouts** — `dag_processing/importers/python_importer.py` and
  `sdk/execution_time/timeout.py` use `signal.setitimer(ITIMER_REAL, ...)` +
  `SIGALRM` to bound Dag import and task execution. `setitimer` does not exist on Emscripten.
  **[EVIDENCE]** Note the importer already tolerates a `ValueError` here ("timeout can't be used in
  the current context"); it does not tolerate `AttributeError`. Widening that guard is a one-line
  upstream fix; a real WASM timeout needs cooperative deadline checks or an out-of-band watchdog
  (a JS timer that terminates the worker), since preemption is impossible in-thread. **[INFERENCE]**

### 3.5 The scheduler loop

`jobs/scheduler_job_runner.py` is a long-lived blocking `while` loop that also starts executors and
(optionally) the Dag processor. In a browser, a blocking loop starves the event loop and freezes the
tab. The replacement is a **cooperative tick**: expose one `async def run_single_loop()` iteration
and drive it from `setTimeout`/`requestIdleCallback`, awaiting between critical sections. The
scheduler's own structure (`_do_scheduling` per loop) is already close to tick-shaped. **[INFERENCE]**

### 3.6 Sketch of a WASM execution profile

```text
JS/browser main thread
 └─ Pyodide (CPython 3.14, wasm32-emscripten, JSPI on)
     ├─ scheduler:      cooperative tick (no blocking loop, no multiprocessing)
     ├─ dag parsing:    DagBag in-process (no DagFileProcessorProcess)
     ├─ execution:      InProcessExecutor -> InProcessTestSupervisor-style runner
     │                    (no fork, no socketpair, no run_as_user, no VirtualEnv)
     ├─ execution API:  FastAPI app called over ASGI in-process (JSPI sync bridge)
     ├─ public API:     same FastAPI app, served to the React UI via a fetch/SW shim
     └─ metadata DB:    SQLite (OPFS) or PGlite via a DBAPI bridge
Web Worker (optional)
 └─ second Pyodide instance for task isolation; comms via postMessage, not sockets
```

Web Workers are the only real isolation primitive: each worker is a separate WASM instance, so a
worker-per-task model recovers *some* of what `fork` gave (crash isolation, cancellation by
`worker.terminate()`), at the cost of re-initialising the interpreter and having no shared memory
with the scheduler unless `SharedArrayBuffer` is available (which requires COOP/COEP headers).
**[INFERENCE]**

## 4. Metadata database

### 4.1 SQLite (what the spike used) — works today

Airflow's SQLite support (`settings.py` maps `aiosqlite` for the async engine) plus Pyodide's
bundled SQLite was enough to run all Alembic migrations and persist DagRuns, TaskInstances and
XComs. **[EVIDENCE]** In MEMFS this is ephemeral; durability options are:

- **OPFS** (Origin Private File System) via `sqlite-wasm`'s VFS — real durable file I/O, but the
  synchronous access handles are only available **inside a Web Worker**, which means the Python
  process holding the SQLAlchemy connection should itself live in a worker. **[INFERENCE]**
- **IndexedDB-backed VFS** — works on the main thread, slower, block-store emulation.
- Pyodide's `IDBFS` mount + periodic `syncfs()` — simplest, coarse-grained.

Caveat: Airflow explicitly does not support SQLite for production concurrency. In a single-user
in-browser deployment there *is* only one writer, so the usual objection largely evaporates.
**[INFERENCE]**

### 4.2 PGlite (`COG-GTM/pglite`) — the Postgres option, and the missing piece

PGlite (inspected at version `0.5.4`) is real PostgreSQL compiled to WASM with a TypeScript client,
`IndexedDB`/OPFS persistence, and a Web Worker wrapper. It gives Postgres semantics (real
transactions, JSONB, `SELECT ... FOR UPDATE`, sequences) which matter because Airflow's scheduler is
written against Postgres/MySQL locking behaviour.

The gap is the **driver**, and it is not small:

1. Python in WASM has **no sockets**, so `psycopg2`/`psycopg`/`asyncpg` cannot connect — even if
   they compiled. `@electric-sql/pglite-socket` solves this for *Node* by exposing a TCP server via
   Node's `net` module; in the browser there is no TCP at all.
2. PGlite does expose `execProtocol` / `execProtocolRaw`, which accept **raw PostgreSQL wire
   protocol** messages and return wire-protocol responses.

So the tractable design is a **DBAPI-level bridge, not a socket**: a pure-Python DBAPI 2.0 module
whose "connection" hands wire-protocol bytes to `execProtocol` through Pyodide's JS FFI (or
`postMessage` to a PGlite worker), and whose cursor decodes the returned `RowDescription`/`DataRow`
frames. Two implementation routes: **[INFERENCE]**

- **Reuse `pg8000`** (pure-Python Postgres driver) and replace only its socket object with a
  JS-backed byte channel that speaks to `execProtocolRaw`. This is the lowest-effort route because
  pg8000 already implements the entire wire protocol and already has a SQLAlchemy dialect
  (`postgresql+pg8000`), so **no new dialect is needed** — only a fake socket.
- Write a thin DBAPI over PGlite's higher-level `query()` JSON results and a custom dialect. Less
  wire-protocol work, but then type coercion, parameter binding styles, `RETURNING`, and server-side
  cursors all become your problem, and SQLAlchemy dialect surface is large.

The first route is strongly preferable, and it also means the async-driver question disappears
(pg8000 is sync — which suits the greenlet-less constraint in §2.2). Alembic then works unchanged.

Blocking behaviour is the subtlety: `execProtocol` is a JS promise, and SQLAlchemy calls the socket
synchronously — so the bridge again depends on **JSPI** (or on running the DB in a worker with
`SharedArrayBuffer` + `Atomics.wait`). This is the same lever as §1.3. **[INFERENCE]**

### 4.3 Recommendation

Ship **SQLite/OPFS first** (already proven), and treat PGlite as phase 2 for fidelity with
production deployments. PGlite becomes *necessary* only when you want scheduler code paths that rely
on Postgres row locking to behave identically.

## 5. Networking, filesystem, time

**Networking.** In Emscripten, Python sockets are absent/broken (`socketpair` → `EINVAL`
**[EVIDENCE]**). The browser offers only `fetch`, WebSocket, and WebRTC — all async, none raw TCP.
Consequences: no `uvicorn` listener, no direct Postgres connection, no provider that needs a raw
socket. Outbound HTTP from a task means routing `requests`/`httpx` through a `fetch`-backed
transport (Pyodide patches `urllib`/`requests` partially; CORS still applies, which silently
excludes most real-world APIs unless proxied). WASI is different: `wasi:sockets` exists in Preview 2
and Wasmtime implements TCP, so a **WASI** deployment *can* have real sockets — one of the few areas
where WASI beats the browser. **[INFERENCE]**

**Filesystem.** Dag files need a VFS. Options: MEMFS (ephemeral, fine for a demo — the spike wrote
`/airflow/dags/wasm_demo.py` into it **[EVIDENCE]**), IDBFS/OPFS for persistence, `mountNodeFS` for
Node, or a Dag bundle fetched over HTTP and unpacked into MEMFS at boot. Airflow's Dag bundle
abstraction is a good fit for the last one, and `universal-pathlib`/`fsspec` are already core
dependencies. Git-based bundles are out (no `git` binary, no sockets). **[INFERENCE]**

**Time and scheduling.** `time.time()`/`monotonic()` work; timezone data works (`tzdata` is in the
closure **[EVIDENCE]**). What does not work is *preemptive* timing: no `SIGALRM`, no
`setitimer`, no thread to watch a deadline. Everything time-driven must therefore be either
cooperative (check a deadline between steps) or externally driven (a JS timer that ticks the
scheduler, or terminates a worker that overran). Also note wall-clock suspension: a backgrounded
browser tab throttles timers to ~1/minute, so a browser "scheduler" will drift and must be
catch-up-tolerant rather than assume it ticks on time. **[INFERENCE]**

## 6. Web UI and API server

Airflow 3's API server is a FastAPI/Starlette app (`api_fastapi/`) normally served by
`uvicorn`/`gunicorn` (`api_fastapi/gunicorn_app.py`, `gunicorn_config.py`), with a Vite/React UI in
`airflow-core/src/airflow/ui/` (React 19, Chakra, React Query, Axios, Monaco).

- `uvicorn`/`gunicorn` are **structurally impossible** in WASM: they need `socket.listen()`, and
  gunicorn additionally forks workers. **[INFERENCE from §1.4]**
- But nothing requires them. The app is ASGI; it can be **called** rather than served. The spike
  drove the Execution API this way, including its `lifespan` (which the stock in-process transport
  handles via `a2wsgi`; `httpx.ASGITransport` does not run lifespan, so it must be run explicitly —
  otherwise you get `AttributeError: 'State' object has no attribute 'svcs_registry'` from the
  `svcs` registry the lifespan installs). **[EVIDENCE]**
- For the **UI**, the clean design is: build the existing React app unchanged, and intercept its
  Axios/`fetch` calls with a **Service Worker** (or an Axios adapter) that forwards each request
  into the in-WASM ASGI app and returns the response. The UI then needs no fork. Auth is the wrinkle
  — the simple auth manager issues JWTs; in a single-user local deployment you would short-circuit
  to a fixed identity rather than run a login flow. **[INFERENCE]**
- Server-sent events / streaming endpoints (the UI uses SSE for some views) need extra care in an
  ASGI-called-directly model, since there is no long-lived connection; polling is the fallback.

## 7. Runtime story: Pyodide vs WASI vs the Component Model

| | **Pyodide (Emscripten)** | **CPython on WASI (Wasmtime/Wasmer)** | **Component Model / WASI 0.2 components** |
| --- | --- | --- | --- |
| Maturity for CPython | highest; official Tier-3 platform `wasm32-emscripten`, huge prebuilt package set (§2.1) | CPython supports `wasm32-wasi` as Tier-3; **no package ecosystem** — every C extension must be cross-compiled yourself | earliest; Python components exist (e.g. `componentize-py`) but not for a stack this size |
| Threads | none by default | none in Preview 2 (`wasi-threads` incomplete) | none |
| Subprocess/fork | none | none | none |
| Sockets | none (only `fetch`/WS via JS) | **yes** (`wasi:sockets`, Wasmtime TCP) | yes, via WASI interfaces |
| Filesystem | MEMFS/IDBFS/OPFS/NodeFS | real preopened dirs — better | virtualised |
| Sync↔async bridge | **JSPI** (proven here) | not needed (host calls are sync) | component async is a different model |
| Runs in a browser tab | **yes** | no (needs a host runtime) | no |
| Best for | the demo, the UI, "Airflow in a tab" | headless portable Airflow, CI, plugin sandboxing | future sandboxed operator/plugin isolation |

Practical reading: **Pyodide is the only viable target today** because the dependency closure
already exists there (109/113 packages). WASI is architecturally *nicer* (real sockets, real files,
no JSPI trick) but you would have to cross-compile `cryptography`, `pydantic-core`, `msgspec`,
`libcst`, `protobuf`, `rpds-py` yourself — that is the bulk of the work, and it is work the Pyodide
project has already done. The Component Model is the right long-term story for *sandboxing
operators* (a task as a component with capability-scoped imports), not for hosting Airflow itself.
**[INFERENCE]**

**Versions.** Pyodide 314 ships CPython 3.14.2; `airflow-core` 3.4.0 requires
`>=3.10, !=3.15`, so 3.14 is in range. **[EVIDENCE]** Airflow 3.x is also the *only* sensible
target: Airflow 2 has no Execution API, no in-process supervisor, and a webserver stack (Flask +
gunicorn) with no in-process path at all. Within 3.x, prefer the newest — the in-process
testing machinery this design leans on keeps improving.

## 8. Roadmap

Effort is in **Devin sessions** (one session ≈ a focused end-to-end work unit).

| Phase | Deliverable / demo | Effort |
| --- | --- | --- |
| **P0 — reproducible spike** | Commit the Node+Pyodide harness from §1 as a `dev/` script; CI-able "parse + run a Dag in WASM" with the 5 shims explicit. Demo: terminal output of §0 | 1 |
| **P1 — remove shims 2/4/5 upstream** | Optional `setproctitle`; timeout backend that degrades when `setitimer` is missing; lazy socketpair in the in-process supervisor; thread-free sync ASGI transport when threading is unavailable. Demo: same run with **no monkeypatching of Airflow** | 2–3 |
| **P2 — `psutil`/`grpcio` removal or shim** | Make `psutil` optional on the no-subprocess path; make the OTel gRPC exporter optional. Demo: clean `micropip install` with zero stub wheels | 2 |
| **P3 — durable metadata DB** | SQLite on OPFS inside a Web Worker; state survives reload. Demo: run a Dag, refresh the page, history is still there | 2 |
| **P4 — browser, not Node** | Same run in Chromium ≥137 with JSPI; Dag bundle fetched over HTTP into the VFS. Demo: **a Dag running in a browser tab** | 2 |
| **P5 — cooperative scheduler** | Tickable scheduler (`run_single_loop` driven by a JS timer) + `InProcessExecutor`; scheduled (not just `dag.test()`) runs, incl. retries. Demo: a `schedule="@hourly"` Dag catching up in-tab | 3–4 |
| **P6 — API + React UI** | Serve the real FastAPI app to the real UI via a Service Worker shim; fixed single-user identity. Demo: **the Airflow UI, in a tab, with no server** | 3–4 |
| **P7 — PGlite backend** | pg8000 + JS byte-channel to `execProtocolRaw`; migrations on PGlite; `postgresql+pg8000` URL. Demo: identical run on Postgres semantics | 4–5 |
| **P8 — task isolation** | Worker-per-task, cancellation via `worker.terminate()`, log capture back to the metadata DB. Demo: kill a runaway task from the UI | 3 |
| | **Total to a credible "Airflow in a tab"** (P0–P6) | **~15–18** |

**Minimum viable WASM Airflow** (the smallest thing worth demoing) = P0 + P3 + P4 + a static UI
read-only view: *load a Dag file into the browser VFS, parse it, run a `PythonOperator` chain with
XCom, persist DagRun/TaskInstance/XCom durably, and render the run in a UI.* Everything except the
UI half of that is already demonstrated in §0. **[EVIDENCE]**

Explicitly **out of scope forever**: `KubernetesExecutor`/`CeleryExecutor`, `BashOperator`,
`PythonVirtualenvOperator`, `run_as_user`, `DockerOperator`, git-backed Dag bundles, the triggerer
(needs async SQLAlchemy → greenlet), remote-log shipping to services without CORS.

## 9. Verdict

### Genuinely blocked (do not attempt)

| # | Blocker | Why it cannot be fixed |
| --- | --- | --- |
| B1 | `fork`/`exec`/`subprocess` | absent in WASM by design; nothing to emulate. Kills `LocalExecutor`, `DagFileProcessorProcess`, the subprocess supervisor, `BashOperator`, `PythonVirtualenvOperator`, `run_as_user`, gunicorn |
| B2 | OS threads (no `SharedArrayBuffer`/pthreads) | `RuntimeError: can't start new thread`. Kills any thread-based transport, watchdog, or threadpool |
| B3 | `greenlet` stack switching | cannot be implemented on `wasm32-emscripten`; kills async SQLAlchemy, `aiosqlite` async engine, `greenback`, and therefore the triggerer |
| B4 | Raw sockets in the browser | no TCP; kills `uvicorn` listeners, `psycopg2`, and socket-based providers. (Not a blocker under **WASI**) |
| B5 | Preemptive timeouts (`SIGALRM`/`setitimer`) | no signals, no watchdog thread; task/import timeouts can only be cooperative or worker-terminated |

### Hard but possible

| Item | Why it's tractable |
| --- | --- |
| Sync Python calling async JS | **JSPI proven** (`can_run_sync: True`, §1.3) |
| Serving the FastAPI API without a server | app is ASGI; call it directly + run lifespan manually (**proven**) |
| Running tasks without a subprocess | `InProcessTestSupervisor` already does it; one socketpair away (**proven with a no-op**) |
| Parsing Dags without a child process | `DagBag` in-process (**proven**) |
| Full metadata schema in WASM | all migrations ran on SQLite (**proven**) |
| Durable storage | OPFS/IDBFS, well-trodden |
| Postgres in WASM | PGlite exists; needs a pg8000-over-`execProtocolRaw` bridge |
| Cooperative scheduler | mechanical refactor of an existing loop into ticks |
| React UI against an in-WASM backend | Service Worker / Axios adapter; no Airflow change |

### Upstream changes that would unblock most of this

These are small, non-invasive, and each is defensible on its own merits (they also make Airflow
easier to embed and to test in-process — the WASM case is a forcing function, not the only
beneficiary):

1. **`InProcessExecutionAPI`: a thread-free synchronous transport** when threading is unavailable
   (`api_fastapi/execution_api/app.py`) — today `.transport` unconditionally starts a loop thread.
2. **Lazy socketpair** in `InProcessTestSupervisor._setup_subprocess_socket()`
   (`task-sdk/.../supervisor.py:2077`) — only create it when a task actually spawns a child.
3. **Timeout backends that degrade**: catch `AttributeError` alongside `ValueError` around
   `signal.setitimer` in `dag_processing/importers/python_importer.py` and
   `sdk/execution_time/timeout.py`, and allow a pluggable cooperative timeout.
4. **Optional `setproctitle`/`psutil`** — guard the imports so a no-subprocess deployment does not
   need C extensions.
5. **An `InProcessExecutor`** as a first-class (even if experimental) executor, and a
   **tickable scheduler entry point** (`run_single_loop()`), so embedders can drive the loop.
6. **Optional OTel gRPC exporter** so `grpcio` is not in the default closure.

None of these require WASM knowledge to review, and all of them are testable on CPython.

### Bottom line

The received wisdom — "Airflow can't run in WASM, it forks and needs Postgres" — is **half right**.
The *distributed* Airflow cannot. But the dependency tree is 96% WASM-ready today, the metadata
schema initialises, Dags parse, the Execution API round-trips, and a two-task Dag with XCom
completes — in WebAssembly, with five shims, four of which are one-liners upstream. A useful,
honest, demoable "Airflow in a tab" is roughly **15–18 Devin sessions** away; a *production*
Airflow in WASM is not on the map, and should not be sold as one.

## Appendix: reproducing the spike

The harness lives outside this repository (Node + npm `pyodide`) and is intentionally not committed
here; P0 in §8 is to commit it under `dev/`. Shape of it:

```bash
npm i pyodide
# stub wheels for psutil, setproctitle, greenlet, grpcio in ./stubs
node --experimental-wasm-stack-switching spike.mjs
```

The stub wheels are **audit instrumentation, not a compatibility fix** — `greenlet`'s stub
deliberately raises on `greenlet()` construction and only implements `getcurrent()`, which is all
SQLAlchemy needs at import time.
