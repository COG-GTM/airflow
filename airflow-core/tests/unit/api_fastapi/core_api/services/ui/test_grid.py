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

from __future__ import annotations

import datetime

from airflow.api_fastapi.core_api.services.ui.grid import (
    GridNodeAgg,
    _aggregate_grid_node_plan,
    _build_grid_node_plan,
    _find_aggregates,
    _merge_node_dicts,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.taskgroup import TaskGroup
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.dag import create_scheduler_dag
from tests_common.test_utils.mock_operators import MockOperator


def test_merge_node_dicts_with_none_new_list():
    """Test merging with None new list doesn't crash.

    Regression test for https://github.com/apache/airflow/issues/61208
    When a TaskGroup is converted to a task, new can be None for some runs.
    """
    current = [{"id": "task1", "label": "Task 1"}]
    new = None

    _merge_node_dicts(current, new)

    assert len(current) == 1
    assert current[0]["id"] == "task1"


def test_merge_node_dicts_preserves_taskgroup_structure():
    """Test TaskGroup structure is preserved when converting to task."""
    current = [
        {
            "id": "task_a",
            "label": "Task A",
            "children": [
                {"id": "task_a.subtask1", "label": "Subtask 1"},
            ],
        }
    ]
    new = [{"id": "task_a", "label": "Task A", "children": None}]

    _merge_node_dicts(current, new)

    # Current structure (TaskGroup) is preserved
    assert len(current) == 1
    assert current[0]["id"] == "task_a"
    assert current[0]["children"] is not None
    assert len(current[0]["children"]) == 1


def test_merge_node_dicts_merges_nested_children():
    """Test merging nodes with nested children."""
    current = [
        {
            "id": "group1",
            "label": "Group 1",
            "children": [
                {"id": "group1.task1", "label": "Task 1"},
            ],
        }
    ]
    new = [
        {
            "id": "group1",
            "label": "Group 1",
            "children": [
                {"id": "group1.task1", "label": "Task 1"},
                {"id": "group1.task2", "label": "Task 2"},
            ],
        }
    ]

    _merge_node_dicts(current, new)

    assert len(current) == 1
    assert current[0]["id"] == "group1"
    assert len(current[0]["children"]) == 2


def test_merge_node_dicts_merges_children_and_appends_new_nodes():
    current = [
        {
            "id": "group",
            "label": "group",
            "children": [{"id": "group.task_a", "label": "task_a"}],
        },
        {"id": "task", "label": "task"},
    ]
    new = [
        {
            "id": "group",
            "label": "group",
            "children": [{"id": "group.task_b", "label": "task_b"}],
        },
        {"id": "new_task", "label": "new_task"},
    ]

    _merge_node_dicts(current, new)

    assert [node["id"] for node in current] == ["group", "task", "new_task"]
    group_children = {child["id"] for child in current[0]["children"]}
    assert group_children == {"group.task_a", "group.task_b"}


def test_merge_node_dicts_preserves_existing_non_group_node_shape():
    current = [{"id": "task", "label": "task"}]
    new = [{"id": "task", "label": "task", "children": [{"id": "task.subtask", "label": "subtask"}]}]

    _merge_node_dicts(current, new)

    assert current == [{"id": "task", "label": "task"}]


def test_merge_node_dicts_large_merge_keeps_unique_nodes():
    current = [{"id": f"group_{i}", "children": [{"id": f"group_{i}.old_task"}]} for i in range(400)]
    new = [{"id": f"group_{i}", "children": [{"id": f"group_{i}.new_task"}]} for i in range(400)]
    new.extend({"id": f"new_task_{i}"} for i in range(400))

    _merge_node_dicts(current, new)

    assert len(current) == 800
    assert {child["id"] for child in current[0]["children"]} == {
        "group_0.old_task",
        "group_0.new_task",
    }
    assert {child["id"] for child in current[-401]["children"]} == {
        "group_399.old_task",
        "group_399.new_task",
    }


def _make_plan_test_serdag():
    with DAG("plan_dag", schedule=None, start_date=datetime.datetime(2025, 1, 1)) as dag:
        start = EmptyOperator(task_id="start")
        with TaskGroup("outer") as outer:
            with TaskGroup("inner") as inner:
                EmptyOperator(task_id="a") >> MockOperator.partial(task_id="mapped").expand(arg1=["x", "y"])
            inner >> EmptyOperator(task_id="b")
        start >> outer
    return create_scheduler_dag(dag)


def _ti_summary(state, start_minute, end_minute, version=1):
    summary = GridNodeAgg()
    summary.add_ti(
        state=state,
        start_date=datetime.datetime(2025, 1, 1, 0, start_minute, tzinfo=datetime.timezone.utc),
        end_date=datetime.datetime(2025, 1, 1, 0, end_minute, tzinfo=datetime.timezone.utc),
        dag_version_number=version,
    )
    return summary


def test_build_grid_node_plan_flattens_task_group_tree_in_post_order():
    root = _make_plan_test_serdag().task_group
    plan = _build_grid_node_plan(root)

    assert [(entry.node_id, entry.type, entry.parent_id) for entry in plan] == [
        ("start", "task", root.node_id),
        ("outer.inner.a", "task", "outer.inner"),
        ("outer.inner.mapped", "mapped_task", "outer.inner"),
        ("outer.inner", "group", "outer"),
        ("outer.b", "task", "outer"),
        ("outer", "group", root.node_id),
    ]
    by_id = {entry.node_id: entry for entry in plan}
    assert [plan[i].node_id for i in by_id["outer.inner"].merge_indices] == [
        "outer.inner.a",
        "outer.inner.mapped",
    ]
    assert [plan[i].node_id for i in by_id["outer"].merge_indices] == ["outer.inner", "outer.b"]
    assert by_id["start"].merge_indices == ()


def test_aggregate_grid_node_plan_reuses_plan_across_runs():
    serdag = _make_plan_test_serdag()
    plan = _build_grid_node_plan(serdag.task_group)
    run_ti_details = [
        {
            "start": _ti_summary(TaskInstanceState.SUCCESS, 0, 1),
            "outer.inner.a": _ti_summary(TaskInstanceState.FAILED, 1, 3),
            "outer.b": _ti_summary(TaskInstanceState.UPSTREAM_FAILED, 4, 5, version=2),
        },
        {"start": _ti_summary(TaskInstanceState.RUNNING, 0, 2)},
    ]

    for ti_details in run_ti_details:
        expected = list(_find_aggregates(serdag.task_group, None, ti_details))
        assert list(_aggregate_grid_node_plan(plan, ti_details)) == expected

    first_run = {node["task_id"]: node for node, _ in _aggregate_grid_node_plan(plan, run_ti_details[0])}
    assert first_run["outer.inner.mapped"]["child_states"] == {"none": 1}
    assert first_run["outer.inner"]["child_states"] == {"failed": 1, "none": 1}
    assert first_run["outer"]["child_states"] == {"failed": 1, "none": 1, "upstream_failed": 1}
    assert first_run["outer"]["dag_version_number"] == 2
    assert "outer.inner" not in run_ti_details[0]

    second_run = {node["task_id"]: node for node, _ in _aggregate_grid_node_plan(plan, run_ti_details[1])}
    assert second_run["outer"]["child_states"] == {"none": 1}
    assert second_run["start"]["state"] == TaskInstanceState.RUNNING
