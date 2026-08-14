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
A scheduler loop for one thread.

``SchedulerJobRunner`` cannot run here: it installs signal handlers, forks a DAG processor, and hands
work to an executor that expects worker processes.  What it *does* around that is reusable, so a tick
here calls the same model methods the scheduler does -- ``DagRun.update_state`` to work out what is
runnable, ``DagRun.schedule_tis`` to mark it scheduled -- and then, instead of queueing to an
executor, runs each task inline via :mod:`airflow_wasm.runner`.

The consequences are deliberate: tasks run one at a time, in the order the scheduler picked them, and
a tick blocks the worker until every runnable task has finished.  Everything the UI reads (dag run
and task instance state, XComs, logs) is written by Airflow's own code.
"""

from __future__ import annotations

import time
from typing import Any

MAX_TASKS_PER_TICK = 32

#: A tick does the work of all three of these, so all three report themselves alive to the UI.
HEARTBEAT_JOB_TYPES = ("SchedulerJob", "TriggererJob", "DagProcessorJob")


def _state_name(state: Any) -> str:
    """Airflow's state enums stringify to ``TaskInstanceState.SUCCESS``; the UI wants ``success``."""
    return "none" if state is None else str(getattr(state, "value", state))


def heartbeat(session: Any) -> None:
    """
    Keep the scheduler, triggerer and dag processor rows alive.

    Airflow derives the health badges in the UI from the ``job`` table, and in this runtime the tick
    that just ran *is* all three components, so each gets a row with a fresh heartbeat.
    """
    from sqlalchemy import select

    from airflow.jobs.job import Job
    from airflow.sdk import timezone
    from airflow.utils.state import JobState

    now = timezone.utcnow()
    for job_type in HEARTBEAT_JOB_TYPES:
        job = session.scalar(select(Job).where(Job.job_type == job_type).order_by(Job.id.desc()))
        if job is None:
            job = Job(job_type=job_type, hostname="browser", unixname="browser", start_date=now)
            session.add(job)
        job.state = JobState.RUNNING
        job.latest_heartbeat = now


def _dag_bag() -> Any:
    from airflow.models.dagbag import DBDagBag

    return DBDagBag()


def create_scheduled_runs(session: Any) -> list[str]:
    """Create dag runs that are due, the way the scheduler's ``_create_dag_runs`` does."""
    from sqlalchemy import select

    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.sdk import timezone
    from airflow.utils.state import DagRunState
    from airflow.utils.types import DagRunTriggeredByType, DagRunType

    now = timezone.utcnow()
    created = []
    dag_bag = _dag_bag()
    query = select(DagModel).where(
        DagModel.is_paused == False,  # noqa: E712
        DagModel.is_stale == False,  # noqa: E712
        DagModel.next_dagrun_create_after <= now,
    )
    for dag_model in session.scalars(query):
        dag = dag_bag.get_latest_version_of_dag(dag_model.dag_id, session=session)
        if dag is None:
            continue
        # Same source of truth the scheduler uses: the timetable derives the next run from the dates
        # already stored on the dag model.
        next_info = dag.timetable.next_run_info_from_dag_model(dag_model=dag_model)
        if next_info is None:
            continue
        run_id = dag.timetable.generate_run_id(
            run_type=DagRunType.SCHEDULED,
            run_after=next_info.run_after,
            data_interval=next_info.data_interval,
            partition_key=next_info.partition_key,
        )
        existing = session.scalar(select(DagRun).where(DagRun.dag_id == dag.dag_id, DagRun.run_id == run_id))
        if existing is not None:
            dag_model.calculate_dagrun_date_fields(dag=dag, reference_run=existing)
            continue
        created_run = dag.create_dagrun(
            run_id=run_id,
            logical_date=next_info.logical_date,
            data_interval=next_info.data_interval,
            run_after=next_info.run_after,
            run_type=DagRunType.SCHEDULED,
            triggered_by=DagRunTriggeredByType.TIMETABLE,
            state=DagRunState.QUEUED,
            session=session,
            partition_key=next_info.partition_key,
            partition_date=next_info.partition_date,
        )
        dag_model.calculate_dagrun_date_fields(dag=dag, reference_run=created_run)
        created.append(f"{dag.dag_id}:{run_id}")
    session.commit()
    return created


def _runnable_dag_runs(session: Any) -> list[Any]:
    from sqlalchemy import select

    from airflow.models.dagrun import DagRun
    from airflow.utils.state import DagRunState

    return list(
        session.scalars(
            select(DagRun)
            .where(DagRun.state.in_([DagRunState.QUEUED, DagRunState.RUNNING]))
            .order_by(DagRun.id)
        )
    )


def _run_one_task(ti: Any, session: Any) -> str:
    """Queue a scheduled task instance and run it inline."""
    from airflow.executors import workloads
    from airflow.sdk import timezone
    from airflow.utils.state import TaskInstanceState
    from airflow_wasm import runner

    # The real scheduler counts the attempt as it schedules the task (see ``DagRun.schedule_tis``), and
    # the log file name the UI asks for is derived from it, so it has to be bumped here too.
    ti.try_number += 1
    ti.state = TaskInstanceState.QUEUED
    ti.queued_dttm = timezone.utcnow()
    ti.scheduled_dttm = ti.scheduled_dttm or ti.queued_dttm
    session.commit()

    workload = workloads.ExecuteTask.make(ti)
    result = runner.run_task(
        workload.ti,
        bundle_name=workload.bundle_info.name,
        rel_path=str(workload.dag_rel_path),
        log_path=workload.log_path,
    )
    session.expire_all()
    return _state_name(result.state)


def tick(*, run_tasks: bool = True) -> dict[str, Any]:
    """Advance the whole system by one step.  Safe to call repeatedly."""
    from airflow.utils.session import create_session
    from airflow.utils.state import DagRunState, TaskInstanceState

    started = time.monotonic()
    summary: dict[str, Any] = {"created_runs": [], "tasks": [], "dag_runs": {}}
    with create_session() as session:
        heartbeat(session)
        summary["created_runs"] = create_scheduled_runs(session)
        dag_bag = _dag_bag()

        for dag_run in _runnable_dag_runs(session):
            dag = dag_bag.get_dag_for_run(dag_run, session=session)
            if dag is None:
                continue
            dag_run.dag = dag
            if dag_run.state == DagRunState.QUEUED:
                dag_run.set_state(DagRunState.RUNNING)
            schedulable, _ = dag_run.update_state(session=session, execute_callbacks=False)
            dag_run.schedule_tis(schedulable, session=session)
            session.commit()

            if run_tasks:
                for ti in dag_run.get_task_instances(state=[TaskInstanceState.SCHEDULED], session=session):
                    if len(summary["tasks"]) >= MAX_TASKS_PER_TICK:
                        break
                    state = _run_one_task(ti, session)
                    summary["tasks"].append({"task_id": ti.task_id, "state": state})

                # Fold the results of those runs into the dag run's own state.
                schedulable, _ = dag_run.update_state(session=session, execute_callbacks=False)
                dag_run.schedule_tis(schedulable, session=session)
                session.commit()
            summary["dag_runs"][f"{dag_run.dag_id}:{dag_run.run_id}"] = _state_name(dag_run.state)
        session.commit()

    summary["duration"] = round(time.monotonic() - started, 3)
    return summary


def run_until_idle(max_ticks: int = 20) -> list[dict[str, Any]]:
    """Tick until nothing moves; handy for tests and for 'run this DAG now' in the UI."""
    history = []
    for _ in range(max_ticks):
        summary = tick()
        history.append(summary)
        if not summary["tasks"] and not summary["created_runs"]:
            break
    return history
