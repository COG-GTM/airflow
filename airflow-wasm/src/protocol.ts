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

/** Messages between the page and the Pyodide worker. */
export type WorkerRequest =
  | { id: number; type: "boot"; baseUrl: string; dataDir?: string }
  | {
      id: number;
      type: "http";
      method: string;
      url: string;
      headers: [string, string][];
      body: ArrayBuffer | null;
    }
  | { id: number; type: "tick" }
  | { id: number; type: "writeDag"; filename: string; source: string };

/** ``Omit`` over a union, so each member keeps its own shape. */
export type Unidentified<T> = T extends { id: number } ? Omit<T, "id"> : never;

export type WorkerResponse =
  | { type: "progress"; text: string }
  | { type: "result"; id: number; result: unknown }
  | { type: "error"; id: number; error: string };

/** What the worker returns for an ``http`` request, and what the service worker turns into a Response. */
export interface HttpResult {
  status: number;
  headers: [string, string][];
  body: ArrayBuffer;
}
