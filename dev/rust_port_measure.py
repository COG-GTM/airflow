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
# /// script
# requires-python = ">=3.10"
# ///
"""
Measurement harness backing ``docs/rust-port-analysis.md``.

Run inside an environment that already has Airflow importable, e.g.::

    uv run --project airflow-core python dev/rust_port_measure.py imports airflow.sdk

Each subcommand prints one JSON document so results can be pasted into the report.
"""

from __future__ import annotations

import gc
import json
import os
import resource
import statistics
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
EXAMPLES = REPO / "airflow-core" / "src" / "airflow" / "example_dags"


def rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def timeit(fn, n=5):
    samples = []
    for _ in range(n):
        gc.collect()
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000)
    return min(samples), statistics.median(samples)


def cmd_imports() -> None:
    mods = [
        "airflow",
        "airflow.sdk",
        "airflow.jobs.scheduler_job_runner",
        "airflow.serialization.serialized_objects",
        "airflow.models.taskinstance",
        "airflow.sdk.execution_time.task_runner",
        "airflow.sdk.execution_time.supervisor",
        "airflow.api_fastapi.app",
    ]
    target = sys.argv[2]
    t0 = time.perf_counter()
    __import__(target)
    dt = (time.perf_counter() - t0) * 1000
    print(
        json.dumps(
            {
                "module": target,
                "known_module": target in mods,
                "import_ms": round(dt, 1),
                "rss_mb": round(rss_mb(), 1),
            }
        )
    )


def cmd_parse_examples() -> None:
    from airflow.dag_processing.dagbag import DagBag

    files = sorted(p for p in EXAMPLES.glob("*.py") if p.name != "__init__.py")
    t0 = time.perf_counter()
    bag = DagBag(dag_folder=os.fspath(EXAMPLES), load_op_links=False)
    total = (time.perf_counter() - t0) * 1000
    print(
        json.dumps(
            {
                "files": len(files),
                "dags": len(bag.dags),
                "tasks": sum(len(d.tasks) for d in bag.dags.values()),
                "total_parse_ms": round(total, 1),
                "ms_per_file": round(total / len(files), 1),
                "rss_mb_after": round(rss_mb(), 1),
                "import_errors": len(bag.import_errors),
            }
        )
    )


def cmd_parse_single() -> None:
    """Parse one file in a fresh interpreter: isolates cold-start from steady-state."""
    from airflow.dag_processing.dagbag import DagBag

    path = sys.argv[2]
    rss_before = rss_mb()
    t0 = time.perf_counter()
    bag = DagBag(dag_folder=path, load_op_links=False)
    dt = (time.perf_counter() - t0) * 1000
    print(
        json.dumps(
            {
                "file": Path(path).name,
                "dags": len(bag.dags),
                "parse_ms": round(dt, 1),
                "rss_mb_before_parse": round(rss_before, 1),
                "rss_mb_after_parse": round(rss_mb(), 1),
            }
        )
    )


def _synthetic_dag(n_tasks: int):
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG

    with DAG(f"synthetic_{n_tasks}", schedule=None) as dag:
        prev = None
        for i in range(n_tasks):
            op = BashOperator(task_id=f"t{i}", bash_command=f"echo {i}")
            if prev:
                prev >> op
            prev = op
    return dag


def cmd_serialize() -> None:
    from airflow.serialization.serialized_objects import DagSerialization

    out = []
    for n in (10, 100, 500, 1000):
        dag = _synthetic_dag(n)
        ser_min, ser_med = timeit(lambda: DagSerialization.to_dict(dag), n=5)
        data = DagSerialization.to_dict(dag)
        blob = json.dumps(data)
        de_min, de_med = timeit(lambda: DagSerialization.from_dict(json.loads(blob)), n=5)
        jl_min, _ = timeit(lambda: json.loads(blob), n=5)
        out.append(
            {
                "tasks": n,
                "serialize_ms": round(ser_min, 1),
                "serialize_median_ms": round(ser_med, 1),
                "json_bytes": len(blob),
                "json_loads_ms": round(jl_min, 2),
                "deserialize_ms": round(de_min, 1),
                "deserialize_median_ms": round(de_med, 1),
            }
        )
    print(json.dumps(out, indent=2))


def cmd_build_dag() -> None:
    """Python-side DAG construction cost only (no file IO, no serialization)."""
    out = []
    for n in (100, 1000):
        b_min, _ = timeit(lambda: _synthetic_dag(n), n=3)
        out.append({"tasks": n, "build_ms": round(b_min, 1)})
    print(json.dumps(out))


def cmd_fork() -> None:
    """Cost of the supervisor's fork of an already-warm interpreter."""
    import socket

    if len(sys.argv) > 2 and sys.argv[2] == "warm":
        from airflow.dag_processing.dagbag import DagBag

        DagBag(dag_folder=os.fspath(EXAMPLES), load_op_links=False)

    samples = []
    for _ in range(20):
        r, w = socket.socketpair()
        t0 = time.perf_counter()
        pid = os.fork()
        if pid == 0:
            try:
                w.send(b"x")
            finally:
                os._exit(0)
        r.recv(1)
        samples.append((time.perf_counter() - t0) * 1000)
        os.waitpid(pid, 0)
        r.close()
        w.close()
    print(
        json.dumps(
            {
                "fork_roundtrip_ms_min": round(min(samples), 2),
                "fork_roundtrip_ms_median": round(statistics.median(samples), 2),
                "parent_rss_mb": round(rss_mb(), 1),
            }
        )
    )


def cmd_orm() -> None:
    """ORM hydration cost for TaskInstance-shaped rows, sqlite in-memory."""
    from sqlalchemy import select

    from airflow.models.taskinstance import TaskInstance

    stmt = select(TaskInstance).where(TaskInstance.state == "scheduled").limit(512)
    c_min, c_med = timeit(lambda: str(stmt.compile()), n=20)
    print(json.dumps({"query_compile_ms_min": round(c_min, 2), "query_compile_ms_median": round(c_med, 2)}))


def cmd_task_child_rss() -> None:
    """RSS of a process holding what a task child holds: SDK runtime + one parsed Dag file."""
    path = sys.argv[2] if len(sys.argv) > 2 else os.fspath(EXAMPLES / "example_bash_operator.py")
    rss_base = rss_mb()
    t0 = time.perf_counter()
    import airflow.sdk.execution_time.task_runner  # noqa: F401

    sdk_ms = (time.perf_counter() - t0) * 1000
    after_sdk = rss_mb()
    from airflow.dag_processing.dagbag import DagBag

    t0 = time.perf_counter()
    bag = DagBag(dag_folder=path, load_op_links=False)
    parse_ms = (time.perf_counter() - t0) * 1000
    print(
        json.dumps(
            {
                "file": Path(path).name,
                "rss_mb_baseline": round(rss_base, 1),
                "sdk_import_ms": round(sdk_ms, 1),
                "rss_mb_after_sdk_import": round(after_sdk, 1),
                "dag_parse_ms": round(parse_ms, 1),
                "rss_mb_after_dag_import": round(rss_mb(), 1),
                "dags": len(bag.dags),
                "tasks": sum(len(d.tasks) for d in bag.dags.values()),
            }
        )
    )


def cmd_gen_dags() -> None:
    """Write ``count`` Dag files of ``tasks`` linear BashOperator tasks into a folder."""
    folder = Path(sys.argv[2])
    count = int(sys.argv[3])
    tasks = int(sys.argv[4])
    folder.mkdir(parents=True, exist_ok=True)
    template = """from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="perf_dag_{i}",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=10),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    prev = None
    for t in range({tasks}):
        op = BashOperator(task_id=f"task_{{t}}", bash_command="echo hi")
        if prev:
            prev >> op
        prev = op
"""
    for i in range(count):
        (folder / f"perf_dag_{i}.py").write_text(template.format(i=i, tasks=tasks))
    print(json.dumps({"folder": os.fspath(folder), "files": count, "tasks_per_dag": tasks}))


def cmd_parse_dir() -> None:
    """Parse + serialize + deserialize every Dag file in a folder."""
    from airflow.dag_processing.dagbag import DagBag
    from airflow.serialization.serialized_objects import DagSerialization

    folder = Path(sys.argv[2])
    files = sorted(p for p in folder.glob("*.py") if p.name != "__init__.py")
    t0 = time.perf_counter()
    bag = DagBag(dag_folder=os.fspath(folder), load_op_links=False)
    parse_ms = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    blobs = [DagSerialization.to_dict(d) for d in bag.dags.values()]
    ser_ms = (time.perf_counter() - t0) * 1000
    payloads = [json.dumps(b) for b in blobs]
    t0 = time.perf_counter()
    for payload in payloads:
        DagSerialization.from_dict(json.loads(payload))
    de_ms = (time.perf_counter() - t0) * 1000
    print(
        json.dumps(
            {
                "files": len(files),
                "dags": len(bag.dags),
                "tasks": sum(len(d.tasks) for d in bag.dags.values()),
                "parse_ms": round(parse_ms, 1),
                "parse_ms_per_file": round(parse_ms / max(len(files), 1), 1),
                "serialize_ms": round(ser_ms, 1),
                "deserialize_ms": round(de_ms, 1),
                "json_bytes_total": sum(len(p) for p in payloads),
                "import_errors": len(bag.import_errors),
                "rss_mb": round(rss_mb(), 1),
            }
        )
    )


def cmd_db_latency() -> None:
    """Round-trip latency of the queries the scheduler critical section issues."""
    from sqlalchemy import select, text

    from airflow.models.pool import Pool
    from airflow.models.taskinstance import TaskInstance as TI
    from airflow.utils.session import create_session

    with create_session() as session:
        select1 = timeit(lambda: session.execute(text("SELECT 1")).scalar(), n=50)
        lock = timeit(lambda: session.execute(text("SELECT pg_try_advisory_xact_lock(1)")).scalar(), n=20)
        stmt = select(TI).where(TI.state == "scheduled").limit(512)
        ti_query = timeit(lambda: session.scalars(stmt).all(), n=20)
        pool_stats = timeit(lambda: Pool.slots_stats(lock_rows=False, session=session), n=20)
    print(
        json.dumps(
            {
                "select1_ms": [round(v, 3) for v in select1],
                "advisory_lock_ms": [round(v, 3) for v in lock],
                "ti_scheduled_query_ms": [round(v, 3) for v in ti_query],
                "pool_slots_stats_ms": [round(v, 3) for v in pool_stats],
            }
        )
    )


def cmd_scheduler_loop() -> None:
    """Run the real scheduler loop against sqlite; count SQL statements and loop latency."""
    from sqlalchemy import event

    from airflow import settings
    from airflow.jobs.job import Job
    from airflow.jobs.scheduler_job_runner import SchedulerJobRunner

    counts = {"n": 0}
    stmts: list[str] = []

    engine = settings.engine

    @event.listens_for(engine, "before_cursor_execute")
    def _count(conn, cursor, statement, parameters, context, executemany):
        counts["n"] += 1
        stmts.append(statement.split("\n")[0][:90])

    num_runs = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    job = Job()
    runner = SchedulerJobRunner(job=job, num_runs=num_runs)
    t0 = time.perf_counter()
    runner._execute()
    dt = (time.perf_counter() - t0) * 1000
    from collections import Counter

    top = Counter(stmts).most_common(12)
    print(
        json.dumps(
            {
                "num_runs": num_runs,
                "total_ms": round(dt, 1),
                "ms_per_loop": round(dt / num_runs, 1),
                "sql_statements": counts["n"],
                "sql_per_loop": round(counts["n"] / num_runs, 1),
                "rss_mb": round(rss_mb(), 1),
                "top_statements": top,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    globals()[f"cmd_{sys.argv[1].replace('-', '_')}"]()
