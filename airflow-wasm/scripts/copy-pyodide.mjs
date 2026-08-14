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

// Pyodide loads its own runtime files (the interpreter wasm, the stdlib zip, package wheels) by URL
// at runtime, so a bundler cannot see them.  Copy the distribution into public/ and point
// loadPyodide() at it, keeping everything on our own origin -- no CDN, no network at boot.

import { cp, mkdir, readdir, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

const require = createRequire(import.meta.url);
const source = dirname(require.resolve("pyodide"));
const target = join(import.meta.dirname, "..", "public", "pyodide");
const { version } = require("pyodide/package.json");
const lock = require("pyodide/pyodide-lock.json");

// The interpreter itself; the npm distribution ships no wheels.
const KEEP = /^(pyodide.*\.(m?js|json|wasm|ts)|python_stdlib\.zip)$/;

// The one package we need before Python can install anything else.  Fetched here rather than at boot
// so the app never depends on the Pyodide CDN being reachable from the browser.
const BUNDLE = ["micropip", "packaging"];

await rm(target, { recursive: true, force: true });
await mkdir(target, { recursive: true });

let copied = 0;
for (const name of await readdir(source)) {
  if (!KEEP.test(name)) continue;
  await cp(join(source, name), join(target, name));
  copied += 1;
}

for (const name of BUNDLE) {
  const { file_name: file } = lock.packages[name];
  const url = `https://cdn.jsdelivr.net/pyodide/v${version}/full/${file}`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: ${response.status}`);
  await writeFile(join(target, file), Buffer.from(await response.arrayBuffer()));
  copied += 1;
}

console.log(`copied ${copied} pyodide files to public/pyodide`);
