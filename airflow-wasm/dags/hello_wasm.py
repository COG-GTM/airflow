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
"""A two-task Dag that proves tasks really execute inside the browser tab."""

from __future__ import annotations

import pendulum

from airflow.sdk import DAG, task

with DAG(
    dag_id="hello_wasm",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["wasm"],
):

    @task
    def where_am_i() -> dict[str, str]:
        import platform
        import sys

        return {
            "python": sys.version,
            "platform": platform.platform(),
            "machine": platform.machine(),
        }

    @task
    def report(environment: dict[str, str]) -> str:
        for key, value in environment.items():
            print(f"{key}: {value}")
        return environment["machine"]

    report(where_am_i())
