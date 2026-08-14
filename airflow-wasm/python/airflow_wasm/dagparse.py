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
Parse DAG files in this interpreter instead of in a DAG processor subprocess.

Airflow's dag processor forks a child per file and ships the serialized result back over a socket.
The pieces either side of that socket are importable, so the browser runtime calls them directly:
``_parse_file`` produces the serialized DAGs and ``update_dag_parsing_results_in_db`` writes the
``dag``/``dag_version``/``serialized_dag``/``import_error`` rows the UI and scheduler read.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

BUNDLE_NAME = "dags-folder"


def dags_folder() -> Path:
    from airflow.configuration import conf

    return Path(conf.get("core", "dags_folder"))


def ensure_bundle() -> None:
    """Register the dags-folder bundle so parsed DAGs have a bundle row to reference."""
    from airflow.dag_processing.bundles.manager import DagBundlesManager

    DagBundlesManager().sync_bundles_to_db()


def parse_file(path: str | Path) -> dict[str, Any]:
    """Parse one DAG file and write the results to the metadata DB."""
    import structlog

    from airflow.dag_processing.collection import update_dag_parsing_results_in_db
    from airflow.dag_processing.processor import DagFileParseRequest, _parse_file
    from airflow.utils.session import create_session

    log = structlog.get_logger(logger_name="dag_processor")
    path = Path(path)
    started = time.monotonic()
    result = _parse_file(
        DagFileParseRequest(
            file=str(path),
            bundle_path=dags_folder(),
            bundle_name=BUNDLE_NAME,
        ),
        log,
    )
    if result is None:
        return {"dags": [], "import_errors": {}}

    relative = str(path.relative_to(dags_folder()))
    import_errors = {(BUNDLE_NAME, relative): err for err in (result.import_errors or {}).values()}
    with create_session() as session:
        update_dag_parsing_results_in_db(
            bundle_name=BUNDLE_NAME,
            bundle_version=None,
            dags=result.serialized_dags,
            import_errors=import_errors,
            parse_duration=time.monotonic() - started,
            warnings=set(),
            session=session,
            files_parsed={(BUNDLE_NAME, relative)},
        )
    return {
        "dags": [dag.dag_id for dag in result.serialized_dags],
        "import_errors": {file: msg for (_, file), msg in import_errors.items()},
    }


def parse_all() -> dict[str, Any]:
    """Parse every ``*.py`` in the dags folder."""
    ensure_bundle()
    dags: list[str] = []
    import_errors: dict[str, str] = {}
    for path in sorted(dags_folder().rglob("*.py")):
        outcome = parse_file(path)
        dags.extend(outcome["dags"])
        import_errors.update(outcome["import_errors"])
    return {"dags": dags, "import_errors": import_errors}


def write_dag(filename: str, source: str) -> dict[str, Any]:
    """Write a DAG file into the (virtual) dags folder and parse it."""
    ensure_bundle()
    path = dags_folder() / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source)
    return parse_file(path)
