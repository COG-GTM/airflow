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

// Resolve Airflow's ~90-wheel dependency tree once, here in Node, and write the wheels plus a Pyodide
// lock file into public/wheels.  The browser then installs Airflow entirely from our own origin: no
// PyPI, no Pyodide CDN, and no dependency resolution at boot.

import { access, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { argv } from "node:process";

import { loadPyodide } from "pyodide";

const require = createRequire(import.meta.url);
const root = join(import.meta.dirname, "..");
const target = join(root, "public", "wheels");
const lockPath = join(target, "pyodide-lock.json");
const pyodideDir = dirname(require.resolve("pyodide"));

const REQUIREMENTS = ["apache-airflow-core==3.3.1", "pg8000"];

if (!argv.includes("--force") && (await exists(lockPath))) {
  console.log("public/wheels is already populated; pass --force to rebuild");
  process.exit(0);
}

async function exists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

const pyodide = await loadPyodide();
await pyodide.loadPackage("micropip");

// The support package is what knows which native dependencies have to be mocked out.
pyodide.FS.mkdirTree("/pkg/airflow_wasm");
const pythonDir = join(root, "python", "airflow_wasm");
for (const name of await readdir(pythonDir)) {
  pyodide.FS.writeFile(`/pkg/airflow_wasm/${name}`, await readFile(join(pythonDir, name)));
}

const frozen = await pyodide.runPythonAsync(`
import json
import sys

sys.path.insert(0, "/pkg")

import micropip

from airflow_wasm import shims

shims.install()
await micropip.install(${JSON.stringify(REQUIREMENTS)}, keep_going=True)
json.dumps({"lock": micropip.freeze(), "installed": sorted(micropip.list())})
`);

const { lock: lockJson, installed } = JSON.parse(frozen);
const lock = JSON.parse(lockJson);

// freeze() describes the whole Pyodide distribution; keep only what Airflow actually pulled in (110
// packages rather than 415).  The set is dependency-closed, so nothing can go looking for the rest.
lock.packages = Object.fromEntries(installed.map((name) => [name, lock.packages[name]]));

// Dependency edges are dropped entirely: some point at shimmed packages Pyodide cannot find
// (greenlet, psutil, ...) and Airflow's own distributions are mutually recursive
// (apache-airflow-core <-> apache-airflow-providers-standard), which deadlocks loadPackage.  The
// browser instead asks for every package in this lock file, so nothing needs resolving.
for (const pkg of Object.values(lock.packages)) {
  pkg.depends = [];
}

// Emptied rather than removed and recreated: a running dev server stops serving a public directory
// that disappears underneath it.
await mkdir(target, { recursive: true });
for (const stale of await readdir(target)) {
  await rm(join(target, stale));
}

let downloaded = 0;
for (const pkg of Object.values(lock.packages)) {
  const source = pkg.file_name;
  const file = source.slice(source.lastIndexOf("/") + 1);
  // PyPI wheels are named by URL; Pyodide's own wheels are already on disk in node_modules.
  const bytes = /^https?:\/\//.test(source)
    ? await download(source)
    : await readFile(source.startsWith("/") ? source : join(pyodideDir, file));
  await writeFile(join(target, file), bytes);
  // Rewritten to a bare name so the browser resolves it against packageBaseUrl (= /wheels/).
  pkg.file_name = file;
  downloaded += 1;
}

async function download(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: ${response.status}`);
  return Buffer.from(await response.arrayBuffer());
}

await writeFile(lockPath, JSON.stringify(lock));
console.log(`wrote ${downloaded} wheels and a lock file to public/wheels`);
