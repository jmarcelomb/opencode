import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { expect } from "bun:test"
import { Effect } from "effect"
import { HttpServer } from "effect/http"
import { it } from "../../core/test/lib/effect"
import { ServerProcess } from "../src/process"

// A2A contextId mappings live in the user's global a2a.db, so keep this test's ids unique
// and route its sessions into the fixture directory through the message metadata.
const CONTEXT = `ctx-test-continuation-${process.pid}`
const NOTICE_ID = `msg_a2a_continuation_${CONTEXT}`
const auth = { authorization: `Basic ${btoa("opencode:secret")}` }

function sendConfig(upstream: string) {
  // Sessions need a resolvable model so their execution reaches the provider request and
  // fails there. 404 is terminal, so no retry loop stalls the executor's completion wait.
  // The agent is pinned too: the user's global config would otherwise resolve the default
  // agent and its real model, making the test dial a live provider.
  return JSON.stringify({
    $schema: "https://opencode.ai/config.json",
    default_agent: "test",
    agents: { test: { description: "Test agent", model: "unreachable/test" } },
    providers: {
      unreachable: {
        name: "unreachable",
        package: "@opencode/ai/providers/openai-compatible",
        settings: { baseURL: upstream, apiKey: "test-key" },
        models: { test: {} },
      },
    },
    model: "unreachable/test",
  })
}

function send(base: string, text: string, directory: string) {
  return fetch(new URL("/api/experimental/a2a", base), {
    method: "POST",
    headers: { ...auth, "content-type": "application/json" },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: text.length,
      method: "SendMessage",
      params: {
        message: {
          kind: "message",
          messageId: `m_${text.length}`,
          role: "user",
          parts: [{ kind: "text", text }],
          contextId: CONTEXT,
          metadata: { directory },
        },
      },
    }),
  }).then(async (response) => {
    const json = (await response.json()) as { result?: { id?: string }; error?: { message?: string } }
    expect(response.status).toBe(200)
    expect(json.error).toBeUndefined()
    return json.result ?? {}
  })
}

function sessions(base: string, directory: string) {
  const query = `?directory=${encodeURIComponent(directory)}`
  return fetch(new URL(`/api/experimental/a2a/sessions${query}`, base), { headers: auth })
    .then((response) => response.json() as Promise<{ data?: Array<{ sessionID: string }> }>)
    .then((json) => json.data ?? [])
}

function archive(base: string, sessionID: string) {
  return fetch(new URL(`/api/session/${sessionID}`, base), {
    method: "PATCH",
    headers: { ...auth, "content-type": "application/json" },
    body: JSON.stringify({ archived: true }),
  }).then(async (response) => {
    expect(response.status).toBe(204)
  })
}

// The notice is either already delivered (projected message) or still pending (inbox item).
async function notice(base: string, sessionID: string) {
  const delivered = await fetch(new URL(`/api/session/${sessionID}/message/${NOTICE_ID}`, base), { headers: auth })
  if (delivered.ok) {
    const json = (await delivered.json()) as { data?: { text?: string }; text?: string }
    return { text: json.data?.text ?? json.text ?? "" }
  }
  await delivered.arrayBuffer()
  const pending = await fetch(new URL(`/api/session/${sessionID}/inbox`, base), { headers: auth })
  const items = ((await pending.json()) as { data?: Array<{ type: string; id: string; payload?: { text?: string } }> })
    .data
  const found = items?.find((item) => item.type === "synthetic" && item.id === NOTICE_ID)
  return found ? { text: found.payload?.text ?? "" } : undefined
}

it.live(
  "injects an A2A continuation notice only when a context's session is archived",
  () =>
    Effect.gen(function* () {
      const fixture = yield* testDirectory()
      const server = yield* ServerProcess.start<never, never>({
        hostname: "127.0.0.1",
        port: 0,
        password: "secret",
        app: { version: "test-version" },
        database: { path: fixture.database },
        pty: { root: fixture.directory },
        fs: { filewatcher: false },
        models: { fetch: false },
      })
      const base = HttpServer.formatAddress(server.address)

      // First message: a fresh context gets a session and no notice.
      yield* Effect.promise(() => send(base, "first", fixture.directory))
      const first = yield* Effect.promise(() => sessions(base, fixture.directory))
      expect(first.length).toBe(1)
      const firstID = first[0]!.sessionID
      expect(yield* Effect.promise(() => notice(base, firstID))).toBeUndefined()

      // Same context while the session is alive: no restart, no notice.
      yield* Effect.promise(() => send(base, "second", fixture.directory))
      expect(yield* Effect.promise(() => notice(base, firstID))).toBeUndefined()

      // Archive the session, then continue the same context: a fresh session carries the notice.
      yield* Effect.promise(() => archive(base, firstID))
      yield* Effect.promise(() => send(base, "third", fixture.directory))
      const after = yield* Effect.promise(() => sessions(base, fixture.directory))
      expect(after.length).toBeGreaterThan(0)
      const secondID = after.find((session) => session.sessionID !== firstID)?.sessionID
      expect(secondID).toBeDefined()
      const injected = yield* Effect.promise(() => notice(base, secondID!))
      expect(injected?.text).toContain(CONTEXT)
      expect(injected?.text).toContain(firstID)
      expect(injected?.text).toContain("archived")
    }),
  // The A2A executor waits for session execution to settle (prompt admission plus drain).
  60000,
)

function restore(flags: string | undefined) {
  if (flags === undefined) delete process.env.OPENCODE_FLAGS
  else process.env.OPENCODE_FLAGS = flags
}

function testDirectory() {
  return Effect.acquireRelease(
    Effect.promise(async () => {
      // The handler re-reads OPENCODE_FLAGS per request, but set it for the whole run anyway.
      const flags = process.env.OPENCODE_FLAGS
      process.env.OPENCODE_FLAGS = "a2a"
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-a2a-continuation-test-"))
      const directory = path.join(root, "dir")
      await fs.mkdir(directory)
      const upstream = Bun.serve({ port: 0, fetch: () => new Response("not found", { status: 404 }) })
      // Location config discovery reads this; the server's config wiring does not reach
      // per-location provider resolution without a file.
      await fs.writeFile(path.join(directory, "opencode.json"), sendConfig(upstream.url.toString()))
      return { flags, database: path.join(root, "opencode.db"), directory, upstream }
    }),
    (fixture) =>
      Effect.promise(async () => {
        restore(fixture.flags)
        await fixture.upstream.stop()
        await fs.rm(path.dirname(fixture.directory), { recursive: true, force: true })
      }),
  )
}
