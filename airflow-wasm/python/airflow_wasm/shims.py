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
Stand-ins for the four Airflow dependencies that have no pure-Python wheel.

Emscripten has no processes to inspect or rename and no way to switch stacks, so ``psutil``,
``setproctitle``, ``grpcio`` and ``greenlet`` can never be installed.  Airflow only reaches them on
code paths the browser runtime does not use (subprocess supervision, ``ps``-style output, gRPC
tracing exporters, SQLAlchemy's sync-over-async bridge), so satisfying the import is enough.

``greenlet`` is deliberately made to raise ``ImportError``: SQLAlchemy treats that as "no greenlet"
and stays on its fully synchronous code paths, whereas a half-working module would let it build
async adapters that then fail deep inside a query.
"""

from __future__ import annotations

PSUTIL_SOURCE = '''
"""Non-functional psutil stand-in for Emscripten."""

import os

version_info = (6, 1, 0)
__version__ = "6.1.0"

POSIX = True
WINDOWS = False
MACOS = False
LINUX = True


class Error(Exception):
    pass


class NoSuchProcess(Error):
    def __init__(self, pid=None, name=None, msg=None):
        super().__init__(msg or f"process no longer exists (pid={pid})")
        self.pid = pid
        self.name = name


class ZombieProcess(NoSuchProcess):
    pass


class AccessDenied(Error):
    pass


class TimeoutExpired(Error):
    def __init__(self, seconds=None, pid=None, name=None):
        super().__init__(f"timeout after {seconds} seconds (pid={pid})")
        self.seconds = seconds
        self.pid = pid
        self.name = name


class Process:
    def __init__(self, pid=None):
        self.pid = os.getpid() if pid is None else pid

    def name(self):
        return "python"

    def cmdline(self):
        return ["python"]

    def username(self):
        return "browser"

    def create_time(self):
        return 0.0

    def is_running(self):
        return self.pid == os.getpid()

    def status(self):
        return "running"

    def children(self, recursive=False):
        return []

    def parent(self):
        return None

    def cwd(self):
        return os.getcwd()

    def environ(self):
        return dict(os.environ)

    def memory_info(self):
        raise NotImplementedError("psutil is not available under Emscripten")

    def cpu_percent(self, interval=None):
        return 0.0

    def num_fds(self):
        return 0

    def open_files(self):
        return []

    def terminate(self):
        pass

    def kill(self):
        pass

    def send_signal(self, sig):
        pass

    def wait(self, timeout=None):
        return 0

    def suspend(self):
        pass

    def resume(self):
        pass


def pid_exists(pid):
    return pid == os.getpid()


def process_iter(attrs=None, ad_value=None):
    return iter(())


def pids():
    return [os.getpid()]


def wait_procs(procs, timeout=None, callback=None):
    procs = list(procs)
    for proc in procs:
        if callback is not None:
            callback(proc)
    return procs, []


def cpu_count(logical=True):
    return 1


def virtual_memory():
    raise NotImplementedError("psutil is not available under Emscripten")
'''

SETPROCTITLE_SOURCE = '''
"""Non-functional setproctitle stand-in: a browser tab has no process title."""

__version__ = "1.3.3"

_title = "python"


def setproctitle(title):
    global _title
    _title = title


def getproctitle():
    return _title


def setthreadtitle(title):
    pass


def getthreadtitle():
    return _title
'''

GREENLET_SOURCE = """
raise ImportError(
    "greenlet cannot be built for Emscripten; SQLAlchemy must stay on its synchronous code paths"
)
"""

GRPCIO_SOURCE = """
raise ImportError("grpcio cannot be built for Emscripten")
"""

GREENBACK_SOURCE = '''
"""greenback stand-in: importable without greenlet, but it cannot teleport coroutines.

Airflow's triggerer uses greenback so that a synchronous ``send()`` on an async generator can
re-enter the running event loop.  Without greenlet that is impossible, so ``has_portal()`` reports
False (the caller then takes its non-greenback path) and the rest raises if actually reached.
"""

__version__ = "1.2.1"


class GreenbackNotAvailable(RuntimeError):
    pass


def has_portal(task=None):
    return False


async def ensure_portal():
    return None


async def with_portal_run(async_fn, *args, **kwargs):
    return await async_fn(*args, **kwargs)


def await_(awaitable):
    raise GreenbackNotAvailable(
        "greenback.await_() needs greenlet, which does not exist under Emscripten"
    )


def bestow_portal(task):
    return None
'''

#: name -> (version, {module: source})
SHIMS: dict[str, tuple[str, dict[str, str]]] = {
    "psutil": ("6.1.0", {"psutil": PSUTIL_SOURCE}),
    "setproctitle": ("1.3.3", {"setproctitle": SETPROCTITLE_SOURCE}),
    "greenlet": ("3.1.1", {"greenlet": GREENLET_SOURCE}),
    "grpcio": ("1.75.1", {"grpc": GRPCIO_SOURCE}),
    "greenback": ("1.2.1", {"greenback": GREENBACK_SOURCE}),
}


def install() -> None:
    """Register the shims with micropip so Airflow's requirements resolve."""
    import micropip

    installed = micropip.list()
    for name, (version, modules) in SHIMS.items():
        if name in installed:
            continue
        micropip.add_mock_package(name, version, modules=modules)
