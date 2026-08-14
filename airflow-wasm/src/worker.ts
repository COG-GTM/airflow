/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// The whole Airflow deployment: interpreter, API server, scheduler, worker and database, in one
// Web Worker.  Requests arrive from the service worker (via the page, which owns this worker),
// scheduler ticks arrive on a timer, and both are handled on the same thread -- see runtime.md for
// why that is not negotiable.

import { PGlite } from "@electric-sql/pglite";

import type { HttpResult, WorkerRequest, WorkerResponse } from "./protocol";

const PYODIDE_PATH = "/pyodide/";
const WHEELS_PATH = "/wheels/";
// In-memory by default.  Airflow talks to PGlite through execProtocolRawSync, and the OPFS-backed
// datadirs answer that call with nothing once a statement has to touch storage, so a persistent
// datadir can be asked for with ?dataDir=... but is not the default.
const DEFAULT_DATA_DIR = "memory://";

// Bundled at build time so the Python side needs no extra network round trips.
const PYTHON_FILES = import.meta.glob("../python/airflow_wasm/*.py", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;
const DAG_FILES = import.meta.glob("../dags/*.py", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;

let pyodide: any;
let webapp: any;
let booted = false;

function progress(text: string): void {
  self.postMessage({ type: "progress", text } satisfies WorkerResponse);
}

function basename(path: string): string {
  return path.slice(path.lastIndexOf("/") + 1);
}

async function packageNames(): Promise<string[]> {
  const response = await fetch(`${WHEELS_PATH}pyodide-lock.json`);
  const lock = (await response.json()) as { packages: Record<string, unknown> };
  return Object.keys(lock.packages);
}

async function openDatabase(dataDir: string): Promise<PGlite> {
  try {
    const db = await PGlite.create({ dataDir });
    await db.waitReady;
    return db;
  } catch (error) {
    // OPFS synchronous access handles need a worker and a browser that supports them; an in-memory
    // database still gives a fully working Airflow, it just forgets everything on reload.
    progress(`could not open ${dataDir} (${error}); falling back to an in-memory database`);
    const db = await PGlite.create();
    await db.waitReady;
    return db;
  }
}

async function boot(dataDir: string, baseUrl: string): Promise<unknown> {
  if (booted) {
    return { alreadyBooted: true };
  }

  progress("loading Python (Pyodide)");
  // Built from location so the bundler treats it as a runtime URL: Pyodide is a copied asset in
  // public/, not part of the module graph.
  const runtime = `${self.location.origin}${PYODIDE_PATH}pyodide.mjs`;
  const { loadPyodide } = await import(/* @vite-ignore */ runtime);
  // scripts/build-wheels.mjs resolved Airflow's dependency tree ahead of time, so installation is a
  // series of same-origin GETs with no resolver and no PyPI.
  const wheels = `${self.location.origin}${WHEELS_PATH}`;
  pyodide = await loadPyodide({
    indexURL: PYODIDE_PATH,
    lockFileURL: `${wheels}pyodide-lock.json`,
    packageBaseUrl: wheels,
  });

  progress("installing the airflow_wasm support package");
  pyodide.FS.mkdirTree("/pkg/airflow_wasm");
  for (const [path, source] of Object.entries(PYTHON_FILES)) {
    pyodide.FS.writeFile(`/pkg/airflow_wasm/${basename(path)}`, source);
  }

  progress("installing Airflow and its dependencies (this is the slow part)");
  await pyodide.loadPackage("micropip");
  await pyodide.runPythonAsync(`
import sys

sys.path.insert(0, "/pkg")

from airflow_wasm import shims

shims.install()
`);
  // The lock file *is* the resolution, so ask for everything in it rather than re-running micropip's
  // resolver: ~110 same-origin wheel fetches and no dependency graph to walk.
  await pyodide.loadPackage(await packageNames());

  progress(`opening the metadata database (${dataDir})`);
  const db = await openDatabase(dataDir);

  progress("initialising the Airflow metadata database");
  webapp = pyodide.pyimport("airflow_wasm.webapp");
  const dags: Record<string, string> = {};
  for (const [path, source] of Object.entries(DAG_FILES)) {
    dags[basename(path)] = source;
  }
  const info = webapp.boot.callKwargs(db, {
    base_url: baseUrl,
    dags: pyodide.toPy(dags),
  });

  booted = true;
  const result = info.toJs({ dict_converter: Object.fromEntries });
  info.destroy();
  return result;
}

async function serve(request: WorkerRequest & { type: "http" }): Promise<HttpResult> {
  // ``webapp.http`` hands back a real JS object (Python-side ``to_js``), including the body as a
  // Uint8Array, so nothing here has to be freed.
  const response = (await webapp.http(
    request.method,
    request.url,
    pyodide.toPy(request.headers),
    pyodide.toPy(new Uint8Array(request.body ?? 0)),
  )) as { status: number; headers: [string, string][]; body: Uint8Array };
  const body = response.body.slice().buffer;
  return { status: response.status, headers: response.headers, body };
}

async function handle(request: WorkerRequest): Promise<unknown> {
  switch (request.type) {
    case "boot":
      return boot(request.dataDir ?? DEFAULT_DATA_DIR, request.baseUrl);
    case "http":
      return serve(request);
    case "tick":
      return webapp.tick();
    case "writeDag":
      return webapp.write_dag(request.filename, request.source);
    default: {
      const exhaustive: never = request;
      throw new Error(`unknown request ${JSON.stringify(exhaustive)}`);
    }
  }
}

// Airflow's own components assume they are the only ones touching the database at a given moment,
// and PGlite really is single-backend, so requests are answered strictly one at a time.
let tail: Promise<void> = Promise.resolve();

self.onmessage = (event: MessageEvent<WorkerRequest>) => {
  const request = event.data;
  tail = tail.then(async () => {
    try {
      const result = await handle(request);
      const message: WorkerResponse = { type: "result", id: request.id, result };
      self.postMessage(message, transferables(result));
    } catch (error) {
      self.postMessage({
        type: "error",
        id: request.id,
        error: String((error as Error)?.message ?? error),
      } satisfies WorkerResponse);
    }
  });
};

function transferables(result: unknown): Transferable[] {
  const body = (result as { body?: unknown })?.body;
  return body instanceof ArrayBuffer ? [body] : [];
}
