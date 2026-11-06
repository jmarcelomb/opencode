export * as A2aClient from "./client.js"

import { Effect, Schema } from "effect"
import { ClientFactory, JsonRpcTransportFactory, type Client } from "@a2a-js/sdk/client"
import { Role, taskStateToJSON, type Task, type Message } from "@a2a-js/sdk"

// One factory for all outbound A2A calls. Transport is JSON-RPC only.
const factory = new ClientFactory({ transports: [new JsonRpcTransportFactory()] })

// SDK types are protobuf-generated and reference internal module paths that
// can't be named in declaration files. TS2883 fires on fetchCard, getTask, and
// cancelTask during `tsc` declaration emit. The CLI build uses Bun.build (not
// tsc) so the binary compiles fine. This is a known @a2a-js/sdk limitation.

// Memoize clients by URL so we don't re-fetch the Agent Card on every call.
// The card fetch is a round-trip to /.well-known/agent-card.json per call; for
// a local peer like Hermes that's fast but wasteful.
const clientCache = new Map<string, Client>()

async function getClient(url: string): Promise<Client> {
  let client = clientCache.get(url)
  if (client) return client
  client = await factory.createFromUrl(url)
  clientCache.set(url, client)
  return client
}

/** Resolve an auth token. Uses the explicit param, or falls back to A2A_HERMES_TOKEN from env. */
function resolveAuth(auth?: string): string | undefined {
  if (auth) return auth
  return process.env.A2A_HERMES_TOKEN
}

/** Build the requestOptions for the SDK client, adding Bearer auth if a token is available. */
function requestOptions(auth?: string) {
  const token = resolveAuth(auth)
  return token ? { serviceParameters: { Authorization: `Bearer ${token}` } } : undefined
}

/** Retry transient connection failures (peer restarting, network blip) with backoff. */
async function withRetry<T>(fn: () => Promise<T>, retries = 2, baseDelay = 500): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await fn()
    } catch (cause) {
      if (attempt >= retries) throw cause
      await new Promise((r) => setTimeout(r, baseDelay * (attempt + 1)))
      // Clear the cache so the next attempt re-fetches the card (peer may have restarted).
      // We don't know the URL here, so clear all — the cache will refill lazily.
      clientCache.clear()
    }
  }
}

/** Fetch a peer's Agent Card from its well-known URL. */
export function fetchCard(baseUrl: string) {
  return Effect.gen(function* () {
    const card = yield* Effect.tryPromise({
      try: () => factory.createFromUrl(baseUrl).then((c) => c.getAgentCard()),
      catch: () => new A2aClientError({ message: `Failed to fetch Agent Card from ${baseUrl}` }),
    })
    const rpcUrl = card.supportedInterfaces?.find((i) => i.protocolBinding === "JSONRPC")?.url ?? baseUrl
    return { name: card.name, description: card.description, url: rpcUrl, skills: card.skills?.map((s) => s.name) }
  })
}

/** Send a non-streaming message to a peer A2A agent. Returns the typed result (Task | Message). */
export const sendMessage = Effect.fn("A2aClient.sendMessage")(function* (input: {
  url: string
  message: string
  auth?: string
  contextId?: string
  metadata?: Record<string, unknown>
}) {
  const client = yield* Effect.tryPromise({
    try: () => getClient(input.url),
    catch: () => new A2aClientError({ message: `Failed to connect to A2A peer at ${input.url}` }),
  })

  const result = yield* Effect.tryPromise({
    try: () =>
      withRetry(() => client.sendMessage({
        message: {
          role: Role.ROLE_USER,
          messageId: `a2a-${crypto.randomUUID()}`,
          contextId: input.contextId ?? "",
          taskId: "",
          parts: [{ content: { $case: "text", value: input.message }, metadata: undefined, filename: "", mediaType: "" }],
          metadata: input.metadata,
          extensions: [],
          referenceTaskIds: [],
        },
        configuration: undefined,
        metadata: undefined,
        tenant: "",
      }, requestOptions(input.auth))),
    catch: (cause) =>
      new A2aClientError({
        message: `A2A peer at ${input.url} returned an error: ${cause instanceof Error ? cause.message : String(cause)}`,
      }),
  })

  return result
})

/** Send a message and return early with a task ID as soon as the peer reports working state.
 *  Uses sendMessageStream and reads the first status event. If the task completes immediately,
 *  returns the completed result instead. */
export const sendMessageAsync = Effect.fn("A2aClient.sendMessageAsync")(function* (input: {
  url: string
  message: string
  auth?: string
  contextId?: string
  metadata?: Record<string, unknown>
}) {
  const client = yield* Effect.tryPromise({
    try: () => getClient(input.url),
    catch: () => new A2aClientError({ message: `Failed to connect to A2A peer at ${input.url}` }),
  })

  const result = yield* Effect.tryPromise({
    try: async () => {
      const stream = client.sendMessageStream({
        message: {
          role: Role.ROLE_USER,
          messageId: `a2a-${crypto.randomUUID()}`,
          contextId: input.contextId ?? "",
          taskId: "",
          parts: [{ content: { $case: "text", value: input.message }, metadata: undefined, filename: "", mediaType: "" }],
          metadata: input.metadata,
          extensions: [],
          referenceTaskIds: [],
        },
        configuration: undefined,
        metadata: undefined,
        tenant: "",
      }, requestOptions(input.auth))

      try {
        // Track IDs across events: the peer may split them over several
        // updates (e.g. a working update, then a terminal one), and any
        // single event can omit one.
        let taskId = ""
        let contextId = ""
        for await (const event of stream) {
          const payload = (event as { payload?: { $case: string; value: { id?: string; contextId?: string; status?: { state?: number; message?: { parts?: { content?: { $case?: string; value?: string } }[] } } } } }).payload
          if (!payload) continue

          if (payload.$case === "task") {
            const task = payload.value
            taskId = task.id || taskId
            contextId = task.contextId || contextId
            const rawState = taskStateToJSON(task.status?.state ?? 0)
            const state = rawState.replace("TASK_STATE_", "").toLowerCase()
            const text = task.status?.message?.parts?.map((p) => (p.content?.$case === "text" ? p.content.value : "")).join("\n") ?? ""

            // Return as soon as we see working state — the task is running.
            if (state === "working") {
              return { state, text: "", taskId, contextId, isTask: true }
            }
            // Terminal state arrived before working — return the result.
            return { state, text, taskId, contextId, isTask: true }
          }

          if (payload.$case === "statusUpdate") {
            const update = payload.value as { status?: { state?: number; message?: { parts?: { content?: { $case?: string; value?: string } }[] } }; taskId?: string; contextId?: string }
            taskId = update.taskId || taskId
            contextId = update.contextId || contextId
            const rawState = taskStateToJSON(update.status?.state ?? 0)
            const state = rawState.replace("TASK_STATE_", "").toLowerCase()
            if (state === "working") {
              // Return early only once we can name the task; otherwise keep
              // streaming and let a later event carry the IDs.
              if (taskId) return { state, text: "", taskId, contextId, isTask: true }
              continue
            }
            // submitted or terminal update: peers end the stream after it,
            // and dropping it would lose the IDs (observed with rejected
            // replies). Return it as the result.
            const text = update.status?.message?.parts?.map((p) => (p.content?.$case === "text" ? p.content.value : "")).join("\n") ?? ""
            return { state, text, taskId, contextId, isTask: true }
          }
        }
        // Stream ended without a terminal state — return working with whatever
        // IDs the updates carried.
        return { state: "working", text: "", taskId, contextId, isTask: true }
      } finally {
        // Cancel the stream generator so the HTTP connection is released.
        await stream.return(undefined as unknown as undefined)
      }
    },
    catch: (cause) =>
      new A2aClientError({
        message: `A2A peer at ${input.url} streaming error: ${cause instanceof Error ? cause.message : String(cause)}`,
      }),
  })

  return result as { state: string; text: string; taskId: string; contextId: string; isTask: boolean }
})

/** Poll a peer for the current state of a task (GetTask). */
export function getTask(input: {
  url: string
  taskId: string
  auth?: string
  historyLength?: number
}) {
  return Effect.gen(function* () {
    const client = yield* Effect.tryPromise({
      try: () => getClient(input.url),
      catch: () => new A2aClientError({ message: `Failed to connect to A2A peer at ${input.url}` }),
    })

    const task = yield* Effect.tryPromise({
      try: () => withRetry(() => client.getTask({ id: input.taskId, tenant: "", historyLength: input.historyLength ?? 0 }, requestOptions(input.auth))),
      catch: (cause) =>
        new A2aClientError({
          message: `A2A peer at ${input.url} getTask failed: ${cause instanceof Error ? cause.message : String(cause)}`,
        }),
    })

    return extractTask(task)
  })
}

/** Cancel a task on a peer (CancelTask). Returns the updated task. */
export function cancelTask(input: {
  url: string
  taskId: string
  auth?: string
}) {
  return Effect.gen(function* () {
    const client = yield* Effect.tryPromise({
      try: () => getClient(input.url),
      catch: () => new A2aClientError({ message: `Failed to connect to A2A peer at ${input.url}` }),
    })

    const task = yield* Effect.tryPromise({
      try: () => withRetry(() => client.cancelTask({ id: input.taskId, tenant: "", metadata: undefined }, requestOptions(input.auth))),
      catch: (cause) =>
        new A2aClientError({
          message: `A2A peer at ${input.url} cancelTask failed: ${cause instanceof Error ? cause.message : String(cause)}`,
        }),
    })

    return extractTask(task)
  })
}

/** Extract text and state from a typed SendMessage result (Task | Message). */
export function extractResult(result: Task | Message): { state: string; text: string; taskId?: string; contextId?: string } {
  if ("status" in result) {
    const rawState = taskStateToJSON(result.status?.state ?? 0)
    const state = rawState.replace("TASK_STATE_", "").toLowerCase()
    const text = result.status?.message?.parts?.map((p) => (p.content?.$case === "text" ? p.content.value : "")).join("\n") ?? ""
    return { state, text, taskId: result.id || undefined, contextId: result.contextId || undefined }
  }
  const text = result.parts?.map((p) => (p.content?.$case === "text" ? p.content.value : "")).join("\n") ?? ""
  return { state: "completed", text, contextId: result.contextId || undefined }
}

/** Extract text and state from a Task (internal helper). */
function extractTask(task: Task): { state: string; text: string; taskId: string } {
  const rawState = taskStateToJSON(task.status?.state ?? 0)
  const state = rawState.replace("TASK_STATE_", "").toLowerCase()
  const text = task.status?.message?.parts?.map((p) => (p.content?.$case === "text" ? p.content.value : "")).join("\n") ?? ""
  return { state, text, taskId: task.id }
}

// --- Error ---

export class A2aClientError extends Schema.TaggedError<A2aClientError>()("A2A.ClientError", {
  message: Schema.String,
}) {}
