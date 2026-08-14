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

// Airflow's UI is a stock single-page app that fetches /airflow/api/v2/... over HTTP; it has no idea
// there is no server.  This worker answers every request under /airflow/ from the Pyodide runtime
// instead of the network: it forwards them over a MessagePort to the page, which owns the runtime.

const PREFIX = "/airflow";
const TIMEOUT_MS = 120_000;

let port = null;
let waiters = [];
const pending = new Map();
let nextId = 1;

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("message", (event) => {
  if (event.data?.type !== "runtime-port") return;
  port = event.data.port;
  port.onmessage = (message) => {
    const { id, ...rest } = message.data;
    const settle = pending.get(id);
    if (!settle) return;
    pending.delete(id);
    settle(rest);
  };
  for (const resolve of waiters.splice(0)) resolve(port);
});

async function runtimePort() {
  if (port) return port;
  // The service worker can be restarted at any time, losing the port; ask the page for a new one.
  const clients = await self.clients.matchAll({ includeUncontrolled: true, type: "window" });
  for (const client of clients) client.postMessage({ type: "need-port" });
  return new Promise((resolve, reject) => {
    waiters.push(resolve);
    setTimeout(() => reject(new Error("the Airflow runtime is not connected")), TIMEOUT_MS);
  });
}

async function proxy(request, url) {
  const target = await runtimePort();
  const body = request.method === "GET" || request.method === "HEAD" ? null : await request.arrayBuffer();
  const id = nextId++;
  const answer = new Promise((resolve) => pending.set(id, resolve));

  target.postMessage(
    {
      id,
      method: request.method,
      url,
      headers: [...request.headers],
      body,
    },
    body ? [body] : [],
  );

  const result = await answer;
  if (result.error) {
    return new Response(`Airflow runtime error: ${result.error}`, {
      status: 500,
      headers: { "content-type": "text/plain" },
    });
  }
  return new Response(result.status === 204 || result.status === 304 ? null : result.body, {
    status: result.status,
    headers: result.headers,
  });
}

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;
  const mounted = url.pathname === PREFIX || url.pathname.startsWith(`${PREFIX}/`);
  // A few of the UI's requests are rooted at the origin rather than at the mount point; they belong
  // to the runtime just the same, so they are forwarded under the prefix the app is mounted on.
  const rooted = url.pathname.startsWith("/api/");
  if (!mounted && !rooted) return;
  const target = mounted ? url.href : `${url.origin}${PREFIX}${url.pathname}${url.search}`;
  event.respondWith(
    proxy(event.request, target).catch(
      (error) =>
        new Response(`Airflow runtime unavailable: ${error.message}`, {
          status: 503,
          headers: { "content-type": "text/plain" },
        }),
    ),
  );
});
