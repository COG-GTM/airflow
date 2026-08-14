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

# Test suite quality review

Independent review of the Airflow test suite, based on the repository state at commit `39ad570`
(`COG-GTM/airflow`, 2026-08-14). Every number below is either **[measured]** — produced by a command
that was actually run in a Linux/Python 3.12.8/8-CPU environment against this checkout — or
**[inferred]** — a judgement drawn from reading code and config. Nothing here is derived from general
knowledge about Airflow-the-product.

Appendix A lists the commands used.

---

## 1. Executive summary and verdict

Airflow has one of the largest and most operationally sophisticated test suites in the Python
ecosystem: **~2,000 Python test files, ~28,700 collected/AST-counted test functions**, plus 875
frontend Vitest tests and a browser end-to-end (E2E) layer, all selected per-change by a bespoke
selective-check engine and split across ~25 CI job families.

The engineering around the suite is excellent. The tests themselves are uneven, and the coverage
signal is effectively absent on pull requests.

**Overall: B / B+.** A strong, credible suite for a project of this scale — with three specific
structural weaknesses that a smaller project would not get away with:

1. **Coverage is measured almost nowhere.** `run_coverage()` returns `true` only for pushes to
   `main` on `apache/airflow` (`dev/breeze/src/airflow_breeze/commands/ci_commands.py:365-372`), and
   even then coverage is disabled for Python 3.12/3.13
   (`dev/breeze/src/airflow_breeze/utils/run_tests.py:595-604`). No contributor and no pull request
   ever sees a coverage number. **[measured]**
2. **Mock density in providers is extreme.** 16,386 `patch`/`mock.patch` sites across `providers/`
   versus 35,101 asserts; 82 test functions carry **six or more** `@patch` decorators. The worst
   examples assert only that mocks were called — change-detector tests that cannot fail for a real
   reason. **[measured]**
3. **The DB-bound half of the suite is the whole cost.** For `providers/standard`, skipping DB tests
   drops wall time from 317 s to 116 s but also drops branch coverage of the provider from 81 % to
   44 %. The cheap, fast, parallel-safe half of the suite exercises less than half the code.
   **[measured]**

What is genuinely better than the Apache-scale norm: the `db_test` / non-DB split with
collection-time deselection, the Helm chart tests (real `helm template` + JSON-schema validation
against real Kubernetes API schemas), query-count regression assertions in the scheduler, an
explicit quarantine mechanism, and a scripted flaky-test analyser that mines historical CI artifacts.

---

## 2. Inventory and structure

### 2.1 Python tests by area [measured]

AST-based counts (`ast.parse` per file, counting `def test_*` including methods):

| Area | Test files | Test functions | `assert` stmts | `pytest.raises` | Parametrized tests | `@patch`-decorated tests | Tests with no assert |
|---|---|---|---|---|---|---|---|
| `airflow-core/tests` | 390 | 6,144 | 13,807 | 1,126 | 1,173 | 705 | 217 |
| `providers/*/tests` | 1,279 | 18,096 | 35,101 | 6,866 | 2,069 | 8,993 | 711 |
| `task-sdk/tests` | 73 | 1,687 | 3,510 | 731 | 240 | 62 | 62 |
| `chart/tests` (Helm) | 76 | 1,263 | 2,206 | 40 | 284 | 0 | 15 |
| `shared/*/tests` | 23 | 395 | 726 | 94 | 58 | 11 | 23 |
| `airflow-ctl/tests` | 15 | 288 | 550 | 124 | 47 | 40 | 19 |
| `dev/breeze/tests` | 59 | 721 | 1,345 | 66 | 116 | 155 | 33 |
| k8s / docker / e2e / integration-only suites | 33 | 142 | 279 | 33 | 13 | 5 | 13 |
| **Total** | **1,948** | **28,736** | **57,524** | **9,080** | **4,000** | **9,971** | **1,093** |

Two more inventories that do not fit the table:

- `find . -name "test_*.py" -o -name "*_test.py"` → **2,004** Python test files; a raw
  `grep -c "def test_"` → **29,557** textual definitions (the delta versus 28,736 is duplicate
  names, non-parsed files, and system-test Dag helpers). **[measured]**
- **771** Python files live under `providers/**/tests/system/**`; **356** files call `watcher()`,
  the marker of an executable system-test Dag. These are excluded from every normal run by the root
  `addopts` (`pyproject.toml:975-976`: `--ignore-glob=**/tests/system/*`). **[measured]**

### 2.2 Frontend tests [measured]

| Layer | Files | Tests | Runner |
|---|---|---|---|
| Component/unit | 117 | **875** | Vitest 4 + happy-dom (`airflow-core/src/airflow/ui/vite.config.ts:71-81`) |
| Browser E2E | 24 specs | ~132 declarations | Playwright, 3 browsers (`airflow-core/src/airflow/ui/playwright.config.ts`) |

### 2.3 Marker inventory [measured]

| Marker | Occurrences | Meaning / effect |
|---|---|---|
| `pytest.mark.db_test` | 945 (366 core, 579 providers) | Requires the metadata DB; deselected by `--skip-db-tests` |
| `pytest.mark.skipif` | 461 | 192 are `not AIRFLOW_V_*` version gates, ~26 are "dependency not installed" |
| `pytest.mark.parametrize` | 4,000 (AST) | — |
| `pytest.mark.backend` | 63 | Postgres/MySQL-specific; counts as a DB test |
| `pytest.mark.integration` | 35 | Requires a live service container |
| `pytest.mark.flaky(reruns=…)` | 16 | Up to `reruns=5` |
| `pytest.mark.skip` | 16 | — |
| `pytest.mark.system` | 11 | — |
| `pytest.mark.xfail` | 7 | Notably low for a suite this size |
| `pytest.mark.quarantined` | 3 sites (1 whole module) | Skipped unless `--include-quarantined` |
| `pytest.mark.long_running` | 1 | — |

### 2.4 Organisation and selection

The suite is a uv workspace monorepo: each distribution (`airflow-core`, `task-sdk`, `providers/*`,
`chart`, `airflow-ctl`, `shared/*`, `dev/breeze`, `scripts`) owns its own `pyproject.toml`, `tests/`
tree, and pytest config. Shared testing infrastructure lives in one place,
`devel-common/src/tests_common/`, which is registered as a pytest plugin and exposes ~47 fixtures in
a **3,294-line** `pytest_plugin.py` plus ~50 helper modules under `test_utils/`. **[measured]**

Selection in CI is three-layered:

1. **`breeze ci selective-check`** maps changed files to test types, providers, backends and Python
   versions.
2. **`scripts/ci/testing/run_unit_tests.sh`** turns a (group, scope) pair into a Breeze invocation —
   `DB` → `--run-in-parallel --run-db-tests-only`, `Non-DB` → `--use-xdist --skip-db-tests
   --no-db-cleanup --backend none`, and a `Quarantined` scope whose failures are swallowed (lines 36-62).
3. **`tests_common.pytest_plugin`** enforces the split at collection time
   (`pytest_collection_modifyitems`, line 890), deselecting rather than skipping so that xdist
   workers never even import DB test modules — the docstring records that runtime skipping OOM'd
   Python 3.14 runners.

`ci-amd.yml` contains **~25 test job families**, including `tests-postgres-core`,
`tests-mysql-providers`, `tests-non-db-core`, `tests-helm`, `tests-kustomize-overlays`,
`tests-integration-system`, `tests-kubernetes`, `tests-task-sdk`, `tests-go-sdk`, `tests-java-sdk`,
`tests-with-lowest-direct-resolution-*`, `migration-round-trip`, and `finalize-tests`. **[measured]**

---

## 3. Taxonomy and infrastructure matrix

| Layer | Where | Exercises | Needs | Runs offline? |
|---|---|---|---|---|
| Non-DB unit | `airflow-core/tests/unit`, `providers/*/tests/unit`, `task-sdk/tests`, `shared/*` | Pure logic: serialization, timetables, param validation, CLI parsing, operator construction | Python only | **Yes** — verified |
| DB unit | The 945 `db_test`-marked tests | Models, scheduler, REST API routes, DB-backed Dag/run/TI lifecycles | SQLite (default), Postgres/MySQL for `backend`-marked | **Yes** with SQLite |
| Integration | `providers/*/tests/integration/**`, 35 `integration` marks | Real client-to-service protocols | Docker service per integration: 3 core (`kerberos`, `otel`, `redis`) + 14 provider (`celery`, `cassandra`, `drill`, `elasticsearch`, `tinkerpop`, `kafka`, `localstack`, `mongo`, `mssql`, `pinot`, `qdrant`, `redis`, `trino`, `ydb`) — `dev/breeze/src/airflow_breeze/global_constants.py:85-101` | No |
| System | 771 files under `providers/**/tests/system` | Real Dags against real cloud APIs | Cloud credentials; excluded from default runs | No |
| Helm/chart | `chart/tests/helm_tests` (1,263 tests), `chart/tests/overlay_tests` | Template rendering for 9 test groups (`airflow_aux`, `airflow_core`, `apiserver`, `dagprocessor`, `redis`, `security`, `statsd`, `webserver`, `other`) | `helm` binary; validated against real K8s JSON schemas (`chart/tests/chart_utils/helm_template_generator.py` uses `subprocess` + `jsonschema.Draft7Validator`) | Yes (no cluster) |
| Kubernetes | `kubernetes-tests/` (5 modules) | Executors and `KubernetesPodOperator` on a live cluster | kind cluster + PROD image | No |
| Docker/compose | `docker-tests/` (4 modules) | PROD/CI image contents, quick-start compose | Docker | No |
| Airflow E2E | `airflow-e2e-tests/` (10+ modules, 926-line conftest) | Whole-stack: event-driven scheduling, remote logging to Elasticsearch/OpenSearch, XCom object storage, Go/TS SDK Dags | Full docker-compose stack | No |
| UI unit | 117 Vitest files | React components, hooks, formatting | Node/pnpm | Yes |
| UI E2E | 24 Playwright specs | Real browser against a running API server | PROD image + browsers | No |
| Static | `.pre-commit-config.yaml` — **134 hooks** | Ruff, mypy per-distribution, license headers, TS/ESLint, spelling, `check-no-new-airflow-exceptions`, selective-check consistency | prek | Yes |

**Measured offline capability:** core collection succeeded for 12,661 tests; `task-sdk` ran
2,769 passed / 6 failed / 8 skipped with no external services; `chart` collected 2,104 tests and
`shared` 619 in under 3 s each. The non-DB and Helm layers are genuinely self-contained; everything
below "Integration" in the table is unavailable to a contributor without Docker. **[measured]**

---

## 4. Quality assessment, with citations

### 4.1 High-quality examples

**Intent-named, parametrized, specific exception matching** —
`task-sdk/tests/task_sdk/definitions/test_param.py:76-100`:

```python
@pytest.mark.parametrize(
    "dt",
    [
        pytest.param("2022-01-02", id="date"),
        pytest.param("03:04:05", id="time"),
        pytest.param("Thu, 04 Mar 2021 05:06:07 GMT", id="rfc2822-datetime"),
    ],
)
def test_string_datetime_invalid_format(self, dt):
    """Test invalid iso8601 and rfc3339 datetime format."""
    with pytest.raises(ParamValidationError, match="is not a 'date-time'"):
        Param(dt, type="string", format="date-time").resolve()
```

Named cases, one behaviour per case, and a `match=` that would catch the exception being raised for
the wrong reason. This is the dominant style in `task-sdk` and it shows: 731 `pytest.raises` for
1,687 tests, and only 62 `@patch`-decorated tests in the entire distribution.

**Contract tests over rendered output, not over mocks** —
`chart/tests/helm_tests/airflow_aux/test_basic_helm_chart.py` renders the chart with a real `helm`
subprocess and asserts against an explicit set of ~45 `(Kind, name)` tuples, then
`validate_k8s_object()` checks each manifest against the JSON schema for the target Kubernetes
version. The chart suite has 2,206 asserts across 1,263 tests, **zero** `@patch` decorators, and
zero `sleep()` calls. It is the highest-quality layer in the repository.

**Performance regression guards** — 182 uses of `assert_queries_count`
(`devel-common/src/tests_common/test_utils/asserts.py:142`), e.g.
`airflow-core/tests/unit/jobs/test_scheduler_job.py:10941`:

```python
with assert_queries_count(expected_query_count, margin=15):
    self.job_runner._do_scheduling(session)
```

Very few Python projects assert on SQL query counts at all. The trade-off is visible in the same
line: `margin=15` is a wide tolerance, and the surrounding test pins `min_serialized_dag_update_interval`
to `100` to stop a background refresh perturbing the count — so the guard catches N+1 regressions but
not modest ones.

### 4.2 Low-quality examples

**Tautological assertion** — `task-sdk/tests/task_sdk/execution_time/test_supervisor.py:1476`:

```python
assert proc.wait() == exit_after or -signal.SIGKILL
```

This parses as `(proc.wait() == exit_after) or (-signal.SIGKILL)`. `-signal.SIGKILL` is `-9`, which is
truthy, so **the assertion can never fail**. The test exercises a real kill-escalation path, and then
throws away the verdict.

**Mock-only change detector** — `providers/teradata/tests/unit/teradata/hooks/test_tpt.py:138-183`:
eleven `@patch` decorators, and the body asserts `result == 0` plus nine
`mock_*.assert_called_once()` calls. Every collaborator is replaced, so the test asserts that the
implementation calls the functions it currently calls — it will fail on any refactor and pass on any
behaviour change that keeps the call sequence. Sibling tests at lines 196 and 317 have **zero**
asserts and eleven patches each. The same pattern recurs in
`providers/openlineage/tests/unit/openlineage/plugins/test_listener.py` (11, 10, 10 and 9 patches at
lines 1457, 2082, 2515, 1821). Across `providers/`, **82** test functions have ≥6 `@patch`
decorators. **[measured]**

**Self-documented timing fragility** —
`task-sdk/tests/task_sdk/execution_time/test_supervisor.py:303`:

```python
# We need a short sleep for the main process to process things. I worry this timing will be
# fragile, but I can't think of a better way. This lets the stdout be read (partial line) and the
# stderr full line be read
sleep(0.1)
```

The comment is honest and the concern is justified: this module also carries
`@pytest.mark.flaky(reruns=3)` at line 389, and both this test and the escalation test above failed
in the xdist run measured in §6.

**Assertion-free tests** — 1,093 test functions contain no `assert` statement (3.8 % of all tests).
Some are legitimate (`pytest.raises`-only, or `assert_queries_count` context managers), but sampling
shows real gaps, e.g. `airflow-core/tests/unit/models/test_dag.py:1663`
`test_validate_partition_key_accepts_exactly_max_length` and
`airflow-core/tests/unit/models/test_dagcode.py:101` `test_write_to_db` — smoke tests whose only
failure mode is an exception. **[measured]**

**Sleeps** — 245 `sleep(` call sites in test code: 198 in `providers/`, 26 in `airflow-core/tests`,
21 in `task-sdk/tests`, 0 in `chart/tests`. **[measured]**

### 4.3 Fixture complexity and test-to-code coupling

- `devel-common/src/tests_common/pytest_plugin.py` is **3,294 lines** with **47 fixtures**, and its
  correctness depends on Airflow not being imported yet — enforced by a module-level
  `assert "airflow" not in sys.modules` at line 78. Any tooling that imports `airflow` before pytest
  plugins load breaks the whole suite (see §5.2). **[measured]**
- **152** `conftest.py` files; the largest are `airflow-e2e-tests` (926 lines) and
  `task-sdk-integration-tests` (743 lines).
- Test modules are very large: `airflow-core/tests/unit/jobs/test_scheduler_job.py` is **13,977
  lines / 314 test functions** with **409** `session.commit()`/`flush()` calls; then
  `test_task_instances.py` (7,757), `test_task_runner.py` (6,793), `test_dag_serialization.py`
  (5,130), `test_dagrun.py` (5,065). Files this size make ownership, review and bisection of a
  failure materially harder. **[measured]**
- 192 `skipif(not AIRFLOW_V_*)` version gates plus a `tests_common/test_utils/compat.py` shim layer
  are the cost of testing ~100 provider distributions against multiple Airflow versions from one
  tree. This is a reasonable design, but it means a large share of provider tests silently do not
  run on any given matrix entry.

---

## 5. Coverage

### 5.1 What is configured, and where it actually runs

- Root `pyproject.toml:1036+` enables branch coverage with `relative_files = true` and omits
  `_vendor`, `contrib`, `example_dags`, and — notably — **`airflow-core/src/airflow/migrations/**`**.
  Migrations are therefore excluded from the coverage number even though
  `migration-round-trip` is a dedicated CI job. **[measured]**
- `codecov.yml:26-38` sets `range: 65..90`, `threshold: 0%`, `after_n_builds: 10`, and restricts the
  project status to `paths: ["airflow"]`.
- `dev/breeze/.../run_tests.py:590-604` disables coverage on Python 3.12 and 3.13, with a comment
  pointing at coverage.py issue 1746 (PEP 669 support): *"coverage that takes too long … causes
  slower execution and occasional timeouts"*.
- `ci_commands.py:365-372`: coverage is enabled **only** for `push` events to `refs/heads/main` on
  `apache/airflow`.

**Consequence [inferred, from the three files above]:** coverage is a post-merge canary metric only.
A pull request that deletes tests, or adds an untested module, produces no coverage signal anywhere
in CI.

### 5.2 Scoped coverage spike [measured]

Branch coverage, `--cov-config=pyproject.toml`, this environment, single-run:

| Scope under test | Tests run | Wall time | Branch coverage of target package |
|---|---|---|---|
| `task-sdk/.../definitions` → `airflow/sdk/definitions` | 785 passed, 3 failed | 17 s | **81 %** |
| `task-sdk/.../execution_time` → `airflow/sdk/execution_time` | 1,101 passed, 1 failed | 54 s | **83 %** |
| `airflow-core/tests/unit/utils` → `airflow/utils`, `--skip-db-tests` | 306 (339 deselected) | 14 s | **44 %** |
| `airflow-core/tests/unit/utils` → `airflow/utils`, DB included | 641 | 76 s | **74 %** |
| `providers/standard` → provider package, `--skip-db-tests` | 415 (770 deselected) | 116 s | **44 %** |
| `providers/standard` → provider package, DB included | 1,044 | 317 s | **81 %** |

Two conclusions fall straight out of the last four rows: where the tests exist, they are good
(74-83 % branch coverage on real modules, which is respectable for code this stateful), and **the
non-DB tier alone covers only ~44 %** of the same code at ~⅓ of the runtime. The fast tier is not a
substitute for the slow one.

A methodological note that is itself a finding: `pytest --cov=airflow.sdk` **cannot run** in this
repository. pytest-cov resolves the module name by importing it, which trips the
`assert "airflow" not in sys.modules` guard in `tests_common/pytest_plugin.py:78`, and the config
bootstrap the plugin performs is skipped — surfacing as a misleading
`yaml.constructor.ConstructorError … config.yml, line 20`. Path-form `--cov=<dir>` works. Airflow's
own CI uses the module form (`--cov=airflow` in `run_tests.py:598`) inside Breeze, where the
environment is pre-configured. **[measured]**

### 5.3 Structurally under-tested areas [inferred, with measured support]

- **Migrations** — omitted from coverage config outright.
- **Scheduler critical sections** — 314 tests in one 13,977-line file, and 84 of them fail as soon as
  they are run concurrently (§6.2). Coverage of `airflow/jobs` is not measured on PRs at all.
- **Executor edge cases** — `kubernetes-tests/` has 5 modules total for the Kubernetes executor
  paths; Celery's executor integration test is `@pytest.mark.flaky(reruns=5, reruns_delay=3)`
  (`providers/celery/tests/integration/celery/test_celery_executor.py:155`).
- **Providers** — mock density means high line coverage of provider glue code coexists with weak
  behavioural coverage. 771 system-test files hold the real API-contract verification, and they are
  ignored by default.
- **UI** — Vitest coverage has no threshold configured (`vite.config.ts:72-74` sets only `include`),
  and 23 of 24 Playwright specs use `test.slow`.

---

## 6. Speed and developer experience

### 6.1 What a contributor must install [measured]

`AGENTS.md` prescribes `uv tool install prek`, `prek install`, and `scripts/tools/setup_breeze`, and
states **"Never run pytest, python, or airflow commands directly on the host — always use breeze."**
Docker is therefore the sanctioned path.

Running on the host anyway (as done for this review) required, on a clean Ubuntu box:

```
sudo apt-get install -y default-libmysqlclient-dev pkg-config \
    libldap2-dev libsasl2-dev libkrb5-dev libsqlite3-dev libpq-dev
uv sync            # 73 s once the headers exist
```

`uv sync` fails outright without those headers — first on `mysqlclient`, then on `python-ldap`. That
is a rough first five minutes for a new contributor who has not read the Breeze docs; the failure
messages do not mention Breeze. **[measured]**

### 6.2 Timings [measured]

| Operation | Result |
|---|---|
| Collect `airflow-core/tests` | 12,661 tests in **31 s** |
| Collect `task-sdk/tests` | 2,783 in 6.6 s |
| Collect `chart/tests` | 2,104 in 2.0 s |
| Collect `shared` / `airflow-ctl` | 619 in 0.8 s / 371 in 0.5 s |
| `task-sdk/tests` serial | 2,769 passed, 6 failed, 8 skipped — **135 s** |
| `task-sdk/tests` with `-n auto` (8 workers) | 2,767 passed, **8** failed — **59 s** (2.3× speed-up, 2 extra failures) |
| `providers/standard` non-DB | 415 tests, 116 s |
| `providers/standard` full | 1,044 tests, 317 s |
| `test_scheduler_job.py` serial, SQLite | **405 passed**, 4 skipped — 49 s |
| `test_scheduler_job.py` with `-n 4`, SQLite | **84 failed**, 321 passed, 2 errors, 10 reruns — 53 s |
| Vitest UI suite (117 files) | 875 passed in **53 s** |
| Vitest with `--coverage` | **1 failed**, 874 passed in 60 s |

Three developer-experience findings from that table:

1. **Parallelism buys 2.3× and costs reliability.** The two extra `task-sdk` failures under xdist are
   the subprocess-timing tests from §4.2 — precisely the ones whose author flagged the risk.
2. **DB tests are not xdist-safe.** The same file passes 405/405 in 49 s serially and fails 84 tests
   in 53 s under `-n 4` on the default SQLite backend — no speed-up, total loss of signal. CI never does this: DB scope uses `--run-in-parallel` (separate containers, separate
   databases) and only the non-DB scope uses `--use-xdist`
   (`scripts/ci/testing/run_unit_tests.sh:36-45`). Correct in CI, but a contributor who reaches for
   `-n auto` on a scheduler change gets a wall of red that has nothing to do with their change.
3. **Instrumentation perturbs the UI suite.** The same Vitest suite passes without `--coverage` and
   fails one test with it. The suite also emits real `ECONNREFUSED` to `127.0.0.1:3000` — unmocked
   network calls that are tolerated because the errors land in unhandled-rejection noise rather than
   assertions.

CI job budgets are generous, which is honest about the real cost: 65 min for unit tests
(`run-unit-tests.yml`), 80 min for Helm and Kustomize, 90 min for UI E2E, 30 min per integration.

---

## 7. Reliability and CI signal

| Mechanism | Evidence |
|---|---|
| In-test reruns | 16 `@pytest.mark.flaky(reruns=…)`, up to `reruns=5` (`providers/standard/tests/unit/standard/triggers/test_external_task.py`, 6 sites) |
| Rerun plugin blocked where it breaks | `task-sdk/tests/conftest.py:45-49` and `airflow-ctl/tests/conftest.py:37-41` call `pluginmanager.set_blocked("rerunfailures")` because it mixes `os.fork` with threads on 3.12 |
| Whole-job retry | `scripts/ci/testing/run_integration_tests_with_retry.sh` retries the entire integration job once, after `sleep 60` and `sudo service docker restart` |
| Quarantine | `--include-quarantined` opt-in flag; quarantined tests skipped by default (`pytest_plugin.py:359-362, 825-829, 939-940`); the Quarantined CI scope appends a shell no-fail guard so it cannot fail the build (`run_unit_tests.sh:51-56`) |
| Playwright retries | `retries: 4` on CI, `0` locally (`playwright.config.ts:110`) |
| Flaky-test analytics | `scripts/ci/analyze_e2e_flaky_tests.py` downloads the last N runs' E2E artifacts across chromium/firefox/webkit and flags any test failing in ≥30 % of runs as a `test.fixme` candidate, posting a Slack report (`e2e-flaky-tests-report.yml`) |
| Failure triage aids | `--maxfail=50`, `--durations=100`, JUnit XML per job, per-test `--execution-timeout`, `finalize-tests` + `notify-slack` jobs |

This is a mature reliability posture, and the honesty is notable: only **3** quarantined sites and
**7** `xfail`s means the project is not hiding failures behind markers at scale. The soft spot is
`retries: 4` for Playwright — a test that only passes on the fifth attempt is indistinguishable from
a passing test in the UI E2E job, and the flaky analyser exists precisely because that signal is
lossy.

---

## 8. Grades

| Dimension | Grade | Justification |
|---|---|---|
| Structure & organisation | **A** | Per-distribution ownership, one shared plugin, collection-time DB/non-DB split, 25 CI job families driven by selective checks. Only deduction: 14k-line test modules. |
| Unit tests (core) | **B** | 6,144 tests, 13,807 asserts, 1,173 parametrized — but 217 assertion-free, and the scheduler's tests are concentrated in one unmaintainably large file. |
| Integration tests | **B+** | 17 real service containers, per-integration jobs, owner mapping so integrations run only for relevant changes. Whole-job retry masks single-test flakiness. |
| Provider tests | **C+** | Largest volume (18,096) and weakest signal: 16,386 patch sites, 82 tests with ≥6 patches, 711 assertion-free, real contract verification exiled to 771 ignored system-test files. |
| Helm/chart tests | **A** | Real `helm template`, JSON-schema validation against real K8s versions, 1,263 tests, no mocks, no sleeps. |
| UI tests | **B−** | 875 Vitest tests pass in 53 s and 24 Playwright specs across 3 browsers — but no coverage threshold, unmocked network calls, `retries: 4`, 23/24 specs `test.slow`. |
| Coverage | **D** | Config is sane; enforcement is absent. Coverage runs only on `main` pushes to `apache/airflow` and never on 3.12/3.13; migrations excluded; no gate anywhere. |
| Speed | **B−** | Excellent collection speed and a real fast tier, but the fast tier covers 44 % of the code, DB tests are not xdist-safe, and CI budgets are 65-90 min. |
| Reliability | **B** | Quarantine, reruns, timeouts, and a genuine flaky-test analyser; only 3 quarantined sites. `\|\| true` on the quarantined scope and Playwright `retries: 4` cost signal. |
| Maintainability | **C+** | 3,294-line plugin with an import-order invariant, 152 conftests, 461 skipifs, 192 version gates, five test files over 5,000 lines. |

**Compared to a mature Apache-scale Python project:** the CI engineering, selective execution, and
multi-backend matrices are well above the norm — this is closer to Kubernetes' test infrastructure
than to a typical ASF Python project. Test *content* is roughly at the norm, dragged down by the
provider tier, and coverage enforcement is **below** the norm: most projects of this profile fail a
PR on a coverage drop, and Airflow cannot even produce the number on a PR.

---

## 9. Prioritized recommendations

Effort is in **Devin sessions** (one session ≈ what an autonomous agent completes end-to-end in one
sitting, including CI iteration).

| # | Recommendation | Expected impact | Effort |
|---|---|---|---|
| 1 | **Make coverage a PR artifact.** Run `--cov` on the non-DB core+task-sdk scope for one Python version on PRs and upload to Codecov as an informational (non-blocking) status. Do not gate yet — just make the number visible. Unblocks every other coverage decision. | Turns coverage from a post-merge curiosity into review information | 1-2 |
| 2 | **Fix and then ban tautological asserts.** Correct `test_supervisor.py:1476`, then add a Ruff/prek check for `assert x == a or <constant>` and for test functions with zero asserts and zero `raises`/context managers, allowlisting the current 1,093 so only new ones fail. | Removes a class of silently-dead tests permanently | 1 |
| 3 | **Cap mock depth in providers.** A prek hook failing any new test function with >4 `patch` decorators (allowlist the existing 82), plus a contributing-docs rule pointing at the `test_tpt.py` pattern as the anti-pattern. | Attacks the single largest quality gap by volume | 1-2 |
| 4 | **Resolve the coverage-on-3.12/3.13 gap.** Re-benchmark coverage.py ≥7.15 under PEP 669 in Breeze and re-enable it if the timing regression is gone; the `run_tests.py` exclusion predates recent coverage releases. | Restores coverage on the versions CI actually defaults toward | 1 |
| 5 | **Make `-n auto` safe, or make it fail loudly.** Either give DB tests per-worker databases (`PYTEST_XDIST_WORKER`-suffixed SQLite files / Postgres schemas) or have the plugin refuse `-n >1` together with DB tests and print the correct Breeze command. 84 spurious failures is a bad first experience. | Removes the biggest local-dev footgun | 2 |
| 6 | **Split the mega-modules.** Mechanically split `test_scheduler_job.py` (13,977 lines) and `test_task_instances.py` (7,757) by concern into a package, preserving test IDs where possible. | Reviewability, ownership, faster bisection | 2-3 |
| 7 | **Promote a handful of system tests into a hermetic contract tier.** For the top ~5 providers, run the existing system-test Dags against local emulators (`localstack` is already a supported integration) in a nightly job, instead of leaving all 771 files ignored. | Real behavioural signal for the weakest tier | 3-4 |
| 8 | **Reduce Playwright `retries: 4` to `1`** and let `analyze_e2e_flaky_tests.py` drive `test.fixme` for the rest. The tooling to absorb the resulting noise already exists. | Restores meaning to a green UI E2E job | 1 |
| 9 | **Add a Vitest coverage floor** for `src/**` (start at the measured value, ratchet), and fail on unhandled network errors in `testsSetup.ts` instead of tolerating `ECONNREFUSED`. | Stops silent UI regressions | 1 |
| 10 | **Un-omit `migrations/**` from coverage** and let `migration-round-trip` report what it actually covers. | Visibility into the riskiest upgrade path | 1 |

---

## Appendix A — commands used

```bash
# inventory
find . -name "test_*.py" -o -name "*_test.py" | wc -l
grep -rc "def test_" --include="test_*.py" . | ...          # 29,557
python3 analyze_tests.py                                    # AST counts per area (table in §2.1)
grep -rn "pytest.mark.db_test" --include=*.py <area> | wc -l
grep -rn "@patch\|mock.patch" --include=*.py <area> | wc -l

# collection
pytest airflow-core/tests --collect-only -q      # 12,661 in 31 s
pytest task-sdk/tests --collect-only -q          # 2,783 in 6.6 s

# execution
pytest task-sdk/tests -q                         # 135 s
pytest task-sdk/tests -q -n auto                 # 59 s
pytest airflow-core/tests/unit/jobs/test_scheduler_job.py -q -n 4

# coverage spikes (path form of --cov is required; see §5.2)
pytest task-sdk/tests/task_sdk/definitions   --cov=task-sdk/src/airflow/sdk/definitions --cov-config=pyproject.toml
pytest airflow-core/tests/unit/utils         --cov=airflow-core/src/airflow/utils --cov-config=pyproject.toml [--skip-db-tests]
pytest providers/standard/tests              --cov=providers/standard/src/airflow/providers/standard --cov-config=pyproject.toml [--skip-db-tests]

# frontend
cd airflow-core/src/airflow/ui && pnpm install --frozen-lockfile && pnpm run test && pnpm run coverage
```

Environment: Ubuntu, Python 3.12.8, 8 CPUs, 31 GB RAM, pytest 9.1.1, pytest-cov 7.1.0,
pytest-xdist 3.8.0, pytest-rerunfailures 16.4, coverage 7.15.2, Vitest 4.1.10, pnpm 10.28.1.

## Appendix B — measurement caveats

- Tests were run on the host in the workspace `.venv`, not inside Breeze, which `AGENTS.md`
  discourages. Absolute timings and a small number of failures (e.g. 3 `test_param.py` datetime
  cases, `deltalake` serializer tests with the package absent, 68 import errors in
  `providers/amazon/tests`) are environment artifacts and are **not** presented as suite defects.
  Structural counts and the relative comparisons (serial vs xdist, DB vs non-DB) are unaffected.
- `pytest providers --collect-only` from the repository root fails with 105 errors
  (`Defining 'pytest_plugins' in a non-top-level conftest is no longer supported`) under pytest 9.
  Per-provider collection works. Airflow pins pytest inside Breeze, so this is a forward-compat
  observation, not a current CI failure.
- No full-suite coverage number was obtained; §5.2 is explicitly a scoped spike.
