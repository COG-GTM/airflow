#!/usr/bin/env python3
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
#
# /// script
# requires-python = ">=3.9"
# ///
"""
Manage a PGlite (WASM Postgres) instance exposed over the Postgres wire protocol.

PGlite normally runs in-process in Node, so nothing can connect to it over TCP.
``@electric-sql/pglite-socket`` ships a ``pglite-server`` CLI that speaks the
Postgres wire protocol on a socket and multiplexes client connections onto the
single PGlite backend, which is what lets Airflow talk to it with a plain
``postgresql+psycopg2://`` connection string.

Usage:
    uv run dev/pglite/pglite_gateway.py start [--fresh]
    uv run dev/pglite/pglite_gateway.py stop
    uv run dev/pglite/pglite_gateway.py status
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

PGLITE_VERSION = os.environ.get("PGLITE_VERSION", "0.5.4")
PGLITE_SOCKET_VERSION = os.environ.get("PGLITE_SOCKET_VERSION", "0.2.7")
GATEWAY_ROOT = Path(os.environ.get("PGLITE_ROOT", Path.home() / ".airflow-pglite"))
HOST = os.environ.get("PGLITE_HOST", "127.0.0.1")
PORT = int(os.environ.get("PGLITE_PORT", "55432"))
# PGlite is a single Postgres backend; the gateway hands it to one client
# transaction at a time, so this bounds queued clients rather than parallelism.
MAX_CONNECTIONS = os.environ.get("PGLITE_MAX_CONNECTIONS", "30")


def is_listening() -> bool:
    with socket.socket() as sock:
        sock.settimeout(1)
        return sock.connect_ex((HOST, PORT)) == 0


def install_packages() -> None:
    if (GATEWAY_ROOT / "node_modules/@electric-sql/pglite-socket").is_dir():
        return
    GATEWAY_ROOT.mkdir(parents=True, exist_ok=True)
    package_json = GATEWAY_ROOT / "package.json"
    if not package_json.exists():
        package_json.write_text(json.dumps({"name": "airflow-pglite", "private": True}) + "\n")
    print(f"Installing pglite {PGLITE_VERSION} and pglite-socket {PGLITE_SOCKET_VERSION}")
    subprocess.run(
        [
            "npm",
            "install",
            "--no-fund",
            "--no-audit",
            f"@electric-sql/pglite@{PGLITE_VERSION}",
            f"@electric-sql/pglite-socket@{PGLITE_SOCKET_VERSION}",
        ],
        cwd=GATEWAY_ROOT,
        check=True,
    )


def start(fresh: bool) -> int:
    if is_listening():
        print(f"Gateway already listening on {HOST}:{PORT}")
        return 0
    install_packages()
    if fresh:
        subprocess.run(["rm", "-rf", str(GATEWAY_ROOT / "data")], check=True)
    log_file = GATEWAY_ROOT / "gateway.log"
    with log_file.open("wb") as log:
        subprocess.Popen(
            [
                "node",
                "node_modules/.bin/pglite-server",
                "--db=./data",
                f"--port={PORT}",
                f"--host={HOST}",
                f"--max-connections={MAX_CONNECTIONS}",
            ],
            cwd=GATEWAY_ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    for _ in range(90):
        if is_listening():
            print(f"PGlite gateway listening on {HOST}:{PORT} (data dir: {GATEWAY_ROOT / 'data'})")
            return 0
        time.sleep(1)
    print("PGlite gateway failed to start. Last log lines:")
    print(log_file.read_text()[-2000:])
    return 1


def stop() -> int:
    subprocess.run(["pkill", "-f", "bin/pglite-server"], check=False)
    time.sleep(2)
    print("PGlite gateway stopped")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["start", "stop", "status"])
    parser.add_argument("--fresh", action="store_true", help="delete the PGlite data directory first")
    args = parser.parse_args()

    if args.command == "start":
        return start(args.fresh)
    if args.command == "stop":
        return stop()
    print("running" if is_listening() else "not running")
    return 0 if is_listening() else 1


if __name__ == "__main__":
    sys.exit(main())
