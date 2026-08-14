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
"""A branching Dag with XComs, so the graph and grid views have something to show."""

from __future__ import annotations

import pendulum

from airflow.sdk import DAG, task

with DAG(
    dag_id="wasm_etl",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wasm"],
):

    @task
    def extract() -> list[int]:
        return [3, 1, 4, 1, 5, 9, 2, 6]

    @task
    def transform(values: list[int]) -> list[int]:
        return sorted(set(values))

    @task
    def summarise(values: list[int]) -> dict[str, int]:
        return {"count": len(values), "total": sum(values), "largest": max(values)}

    @task
    def load(summary: dict[str, int]) -> None:
        print(f"loaded {summary}")

    unique = transform(extract())
    load(summarise(unique))
