import { Session } from "@opencode/core/session"
import { Config } from "@opencode/core/config"
import { Bus } from "@opencode/core/bus"
import { Effect, Fiber, Option, Ref, Schema, Stream } from "effect"
import { AbsolutePath } from "@opencode/core/schema"
import { App } from "@opencode/core/app"
import { ServerInfo } from "../server-info"
import { EventFeed } from "../event-feed"
import { HttpServerRequest } from "effect/http"
import { A2aDbService } from "./a2a-db"
import {
  AgentCard as SdkAgentCard,
  AgentInterface,
  TaskState,
  Role,
  A2A_PROTOCOL_VERSION,
  type Part,
  type Message as A2aMessage,
  type TaskStatus,
} from "@a2a-js/sdk"
import {
  DefaultRequestHandler,
  JsonRpcTransportHandler,
  AgentEvent,
  defaultServerCallContextBuilder,
  type AgentExecutor,
  type TaskStore,
} from "@a2a-js/sdk/server"
import type { Task, ListTasksRequest, ListTasksResponse } from "@a2a-js/sdk"
import type { ServerCallContext } from "@a2a-js/sdk/server"
import { Duration } from "effect"
// --- SqliteTaskStore: persists task state in a2a.db so GetTask survives restarts ---

class SqliteTaskStore implements TaskStore {
  constructor(private readonly a2aDb: { saveTaskState: (id: string, state: string) => Promise<void>; loadTaskState: (id: string) => Promise<string | undefined>; listTaskStates: (pageSize: number, pageToken: string) => Promise<{ states: { taskId: string; state: string }[]; nextPageToken: string }> }) {}

  async save(task: Task, _context: ServerCallContext): Promise<void> {
    await this.a2aDb.saveTaskState(task.id, JSON.stringify(task))
  }

  async load(taskId: string, _context: ServerCallContext): Promise<Task | undefined> {
    const state = await this.a2aDb.loadTaskState(taskId)
    if (!state) return undefined
    try {
      return JSON.parse(state) as Task
    } catch {
      return undefined
    }
  }

  async list(params: ListTasksRequest, _context: ServerCallContext): Promise<ListTasksResponse> {
    const pageSize = params.pageSize ?? 50
    const pageToken = params.pageToken ?? ""
    const { states, nextPageToken } = await this.a2aDb.listTaskStates(pageSize, pageToken)
    const tasks: Task[] = []
    for (const { state } of states) {
      try {
        tasks.push(JSON.parse(state) as Task)
      } catch {
        // Skip corrupted entries.
      }
    }
    return { tasks, nextPageToken, pageSize, totalSize: tasks.length }
  }
}

// --- Errors ---

class A2aDisabledError extends Schema.TaggedError<A2aDisabledError>()("A2A.DisabledError", {
  message: Schema.String,
}) {
  constructor() {
    super({ message: "A2A is not enabled. Set experimental.a2a: true or OPENCODE_FLAGS=a2a." })
  }
}

class A2aUnauthorizedError extends Schema.TaggedError<A2aUnauthorizedError>()("A2A.UnauthorizedError", {
  message: Schema.String,
}) {}

// --- Helpers ---

function textPart(text: string): Part {
  return { content: { $case: "text", value: text }, metadata: undefined, filename: "", mediaType: "" }
}

function agentMessage(messageId: string, contextId: string, taskId: string, text: string): A2aMessage {
  return { messageId, contextId, taskId, role: Role.ROLE_AGENT, parts: [textPart(text)], metadata: undefined, extensions: [], referenceTaskIds: [] }
}

function failedTask(taskId: string, contextId: string, message?: string) {
  return AgentEvent.task({
    id: taskId, contextId,
    status: { state: TaskState.TASK_STATE_FAILED, message: message ? agentMessage("error", contextId, taskId, message) : undefined, timestamp: new Date().toISOString() } as TaskStatus,
    artifacts: [], history: [], metadata: undefined,
  })
}

function workingTask(taskId: string, contextId: string) {
  return AgentEvent.task({
    id: taskId, contextId,
    status: { state: TaskState.TASK_STATE_WORKING, message: undefined, timestamp: new Date().toISOString() } as TaskStatus,
    artifacts: [], history: [], metadata: undefined,
  })
}

/**
 * `a2a.*` in the `server.experimental` group. Every route needs `experimental.a2a` enabled.
 *
 * An inbound A2A SendMessage maps to a `Session.prompt` admission. The A2A contextId resolves
 * to an OpenCode session via a dedicated SQLite database (a2a.db, separate from the main
 * opencode.db). If the context was seen before, the existing session is reused (preserving
 * conversation history); if not, a new session is created. Multiple tasks in the same context
 * share one session. Independent tasks (no contextId) each get a fresh session.
 *
 * One execution busy period (started→terminal) maps to one A2A task. Streaming publishes
 * intermediate text deltas as artifact updates. Parallel sessions: different contexts run
 * concurrently on different sessions. The SDK creates a separate event bus per task.
 *
 * The main database is touched only through the public Session.Service API. All A2A-specific
 * state (contextId→sessionID, taskId→sessionID) lives in a2a.db, decoupled from core migrations.
 */
export const makeA2a = Effect.gen(function* () {
  const session = yield* Session.Service
  const bus = yield* Bus.Service
  const feed = yield* EventFeed.make(bus.listen)
  const a2aDb = yield* A2aDbService
  const db = a2aDb

  // Capture the server's runtime context so the AgentExecutor (a plain async function called by
  // the SDK) can run Effects with all server services (Session, Config, etc.) available.
  const context = yield* Effect.context()
  const run = Effect.runPromiseWith(context)

  /** Read the structured A2A config. */
  const readA2aConfig = Effect.fnUntraced(function* () {
    const config = yield* Effect.serviceOption(Config.Service)
    if (Option.isNone(config)) return { tokens: [] as string[], peers: [] as unknown[], directory: undefined as string | undefined, timeout: undefined as Duration.Duration | false | undefined }
    const entries = yield* config.value.entries()
    const experimental = Config.latest(entries, "experimental")
    const a2aConfig = experimental?.a2a
    if (typeof a2aConfig === "object" && a2aConfig !== null) {
      return {
        tokens: a2aConfig.tokens ?? [],
        peers: a2aConfig.peers ?? [],
        directory: a2aConfig.directory as string | undefined,
        timeout: a2aConfig.timeout as Duration.Duration | false | undefined,
      }
    }
    return { tokens: [] as string[], peers: [] as unknown[], directory: undefined as string | undefined, timeout: undefined as Duration.Duration | false | undefined }
  })

  /** Check whether A2A is enabled. */
  const a2aEnabled = Effect.gen(function* () {
    const flags = (process.env.OPENCODE_FLAGS ?? "").split(",").map((f) => f.trim()).filter(Boolean)
    if (flags.includes("a2a") || flags.includes("all")) return true
    const config = yield* Effect.serviceOption(Config.Service)
    if (Option.isSome(config)) {
      const entries = yield* config.value.entries()
      const experimental = Config.latest(entries, "experimental")
      const a2aConfig = experimental?.a2a
      if (a2aConfig === true || (typeof a2aConfig === "object" && a2aConfig !== null)) return true
    }
    const app = yield* Effect.serviceOption(App.Metadata)
    const channel = Option.isSome(app) ? app.value.channel : "unknown"
    return channel === "local" || channel === "dev"
  })

  /** Bearer token auth. Accepts an HttpServerRequest or a plain object with headers.
   *  Security: loopback connections are allowed without a token when no tokens are configured.
   *  Non-loopback connections ALWAYS require a valid bearer token. If no tokens are configured,
   *  non-loopback is rejected outright. */
  const authorize = (request: unknown) =>
    Effect.gen(function* () {
      const cfg = yield* readA2aConfig()
      const req = request as { headers?: { authorization?: string; get?: (name: string) => string | null }; remoteAddress?: { _tag: string; value?: string } }
      const headers = req.headers
      const authHeader = headers?.authorization ?? (typeof headers?.get === "function" ? (headers.get("authorization") ?? "") : "")
      const bearerMatch = /^Bearer\s+(.+)$/i.exec(authHeader)
      const token = bearerMatch?.[1]

      if (cfg.tokens.length === 0) {
        const remote = (req.remoteAddress?._tag === "Some" ? req.remoteAddress.value : undefined) as string | undefined
        const isLoopback = remote === "127.0.0.1" || remote === "::1" || remote === "::ffff:127.0.0.1"
        if (!isLoopback) return yield* Effect.fail(new A2aUnauthorizedError({ message: "A2A requires a bearer token for non-loopback access" }))
        return
      }
      if (!token || !cfg.tokens.includes(token))
        return yield* Effect.fail(new A2aUnauthorizedError({ message: "Invalid or missing A2A bearer token" }))
    })

  /** Extract text from A2A message parts. */
  const extractText = (message: unknown): string => {
    if (typeof message !== "object" || message === null) return ""
    const parts = (message as { parts?: unknown[] }).parts
    if (!Array.isArray(parts)) return ""
    return parts.map((p) => {
      if (typeof p !== "object" || p === null) return ""
      const content = (p as { content?: { $case?: string; value?: string } }).content
      return content?.$case === "text" ? content.value ?? "" : ""
    }).join("\n")
  }

  /**
   * Resolve an A2A contextId to an OpenCode session.
   * Checks the dedicated a2a.db for an existing mapping. If found, verifies the session
   * still exists via Session.Service. If not found or session gone, creates a new session
   * and stores the mapping.
   */
  const resolveContext = async (contextId: string, directory?: string): Promise<Session.ID> => {
    const existing = await db.resolveContext(contextId)
    if (existing) {
      // Verify the session still exists and is not archived.
      const info = await run(
        session.get(existing.sessionID as Session.ID).pipe(Effect.catchTag("Session.NotFoundError", () => Effect.succeed(undefined))),
      )
      if (info && info.time.archived === undefined) return existing.sessionID as Session.ID
      // Session is gone or archived — create a fresh one for this context.
    }

    // Resolve directory: per-message metadata > config > process.cwd()
    const configDir = await run(readA2aConfig()).then((c) => c.directory)
    const dir = directory ?? configDir ?? process.cwd()
    const created = await run(
      session.create({ location: { directory: AbsolutePath.make(dir) } }),
    )
    // A2A sessions are unattended: no interactive user to approve permission asks.
    // Auto-allow all actions so the session doesn't hang on the first edit or
    // shell command. This only affects A2A-created sessions.
    try {
      await run(
        session.setPermissions({
          sessionID: created.id as Session.ID,
          permissions: [{ action: "*", resource: "*", effect: "allow" }],
        }),
      )
    } catch {
      // Permissions override failed; session still works for read-only tasks.
    }
    await db.saveContext({
      contextId,
      sessionID: created.id,
      directory: dir,
      createdAt: Date.now(),
    })
    return created.id
  }

  /** Build the A2A Agent Card. */
  const card = Effect.fnUntraced(function* () {
    const on = yield* a2aEnabled
    if (!on) return yield* Effect.fail(new A2aDisabledError())
    const info = yield* ServerInfo.Service
    const app = yield* App.Metadata
    const urls = info.urls()
    const loopback = urls.find((u) => u.includes("127.0.0.1") || u.includes("[::1]")) ?? urls[0]
    if (!loopback) return yield* Effect.fail(new A2aDisabledError())
    const rpcUrl = `${loopback}/api/experimental/a2a`
    return {
      name: "opencode",
      description: "opencode coding agent server",
      supportedInterfaces: [{ url: rpcUrl, protocolBinding: "JSONRPC", protocolVersion: A2A_PROTOCOL_VERSION, tenant: "" } as AgentInterface],
      provider: { organization: "opencode", url: "https://opencode.ai" },
      version: app.version ?? "unknown",
      documentationUrl: "https://opencode.ai/docs",
      capabilities: { streaming: true, pushNotifications: false, extensions: [] },
      defaultInputModes: ["text/plain"],
      defaultOutputModes: ["text/plain"],
      skills: [{ id: "coding", name: "coding agent", description: "Coding agent for software development", tags: ["coding", "software"], examples: [], inputModes: [], outputModes: [], securityRequirements: [] }],
      securitySchemes: {},
      securityRequirements: [],
      signatures: [],
    } satisfies SdkAgentCard
  })

  /** List local sessions by working directory. */
  const sessions = Effect.fnUntraced(function* (directory?: string) {
    const on = yield* a2aEnabled
    if (!on) return yield* Effect.fail(new A2aDisabledError())
    // sessions is called from .handle() where HttpServerRequest is available
    const request = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest)
    const req = Option.isSome(request) ? request.value : { headers: {}, remoteAddress: { _tag: "None" } as const }
    yield* authorize(req)
    const info = yield* ServerInfo.Service
    const urls = info.urls()
    const loopback = urls.find((u) => u.includes("127.0.0.1") || u.includes("[::1]")) ?? urls[0]
    const a2aUrl = `${loopback}/api/experimental/a2a`
    const result = yield* session.list(directory ? { directory: AbsolutePath.make(directory) } : undefined)
    const active = yield* session.active
    return result.data.map((s) => ({
      sessionID: s.id,
      title: s.title,
      a2aUrl,
      state: s.time.archived !== undefined ? "archived" as const : active.has(s.id) ? "working" as const : "idle" as const,
    }))
  })

  /** Remove orphaned opencode sessions for stale A2A contexts, then prune the DB rows. */
  const cleanupStaleSessions = Effect.fnUntraced(function* () {
    const cutoff = Date.now() - 24 * 60 * 60 * 1000
    const stale = yield* Effect.tryPromise(() => db.staleContexts(cutoff))
    if (stale.length === 0) return
    const removals = stale.map((ctx) =>
      session.remove(ctx.sessionID as Session.ID).pipe(
        Effect.match({
          onFailure: (cause) => {
            console.warn(`[a2a] failed to remove stale session ${ctx.sessionID} for context ${ctx.contextId}:`, cause instanceof Error ? cause.message : cause)
          },
          onSuccess: () => {},
        }),
      ),
    )
    yield* Effect.all(removals, { concurrency: "unbounded" })
    yield* Effect.tryPromise(() => db.prune())
  })

  // --- AgentExecutor ---

  const executor: AgentExecutor = {
    execute: async (requestContext, eventBus) => {
      const { request, taskId, contextId } = requestContext
      const message = request.message
      if (!message) {
        eventBus.publish(failedTask(taskId, contextId, "Missing message"))
        return
      }

      // Resolve session from contextId via the dedicated A2A database.
      // Accept directory from metadata.opencode.directory (from opencode peers) or metadata.directory (generic).
      const directory = (message.metadata?.opencode?.directory ?? message.metadata?.directory) as string | undefined
      let sessionID: Session.ID
      try {
        sessionID = await resolveContext(contextId, directory)
      } catch (cause) {
        eventBus.publish(failedTask(taskId, contextId, `Failed to resolve session: ${cause instanceof Error ? cause.message : String(cause)}`))
        return
      }

      // Persist task→session mapping for CancelTask.
      await db.saveTask({ taskId, contextId, sessionID, createdAt: Date.now() })

      // Opportunistically clean up stale sessions (non-blocking).
      run(cleanupStaleSessions().pipe(Effect.ignore)).catch(() => {})

      const text = extractText(message)
      if (!text) {
        eventBus.publish(failedTask(taskId, contextId, "Empty message text"))
        return
      }

      eventBus.publish(workingTask(taskId, contextId))

      // Subscribe to EventFeed BEFORE admitting the prompt so we don't miss early deltas.
      const eventStream = await run(
        Effect.scoped(feed.subscribe([
          "session.execution.succeeded",
          "session.execution.failed",
          "session.execution.interrupted",
          "session.text.delta",
        ])),
      )

      // Pump text deltas into the A2A event bus as artifact updates for live streaming.
      const pumpFiber = Effect.runFork(
        eventStream.pipe(
          Stream.map((raw: string) => {
            const match = raw.match(/^data: (.+)$/s)
            if (!match) return null
            try {
              return JSON.parse(match[1]) as { type: string; data: Record<string, unknown> }
            } catch {
              return null
            }
          }),
          Stream.filter((event): event is { type: string; data: Record<string, unknown> } => event !== null),
          Stream.filter((event) => event.data.sessionID === sessionID),
          Stream.runForEach((event) =>
            Effect.sync(() => {
              if (event.type === "session.text.delta") {
                eventBus.publish(
                  AgentEvent.artifactUpdate({
                    taskId,
                    contextId,
                    artifact: {
                      artifactId: `text-${event.data.assistantMessageID ?? "stream"}`,
                      name: "",
                      description: "",
                      parts: [textPart(event.data.delta as string)],
                      metadata: undefined,
                      extensions: [],
                    },
                    append: true,
                    lastChunk: false,
                    metadata: undefined,
                  }),
                )
              }
            }),
          ),
        ).pipe(Effect.ignore),
      )

      // Admit the prompt and wait for the execution to complete, with a configurable timeout.
      await run(
        session.prompt({ sessionID, text, delivery: "steer", metadata: { a2a: { taskId, contextId } } }).pipe(Effect.ignore),
      )
      const cfg = await run(readA2aConfig())
      const waitEffect = session.wait(sessionID)
      const timeoutDuration = cfg.timeout === false ? undefined : cfg.timeout ?? Duration.minutes(10)
      await run(
        (timeoutDuration
          ? waitEffect.pipe(Effect.timeout(timeoutDuration))
          : waitEffect
        ).pipe(Effect.ignore),
      )

      // Stop the event pump.
      await run(Fiber.interrupt(pumpFiber).pipe(Effect.ignore))

      // Read the outcome.
      const info = await run(
        session.get(sessionID).pipe(Effect.catchTag("Session.NotFoundError", () => Effect.succeed(undefined))),
      )
      const outcome = info?.outcome ?? "succeeded"
      const state: TaskState = outcome === "failed"
        ? TaskState.TASK_STATE_FAILED
        : outcome === "interrupted"
          ? TaskState.TASK_STATE_CANCELED
          : TaskState.TASK_STATE_COMPLETED

      // Read the last assistant message for the reply.
      const messages = await run(
        session.messages({ sessionID, limit: 10, order: "desc" as const }).pipe(
          Effect.catchTag("Session.NotFoundError", () => Effect.succeed([])),
        ),
      )
      const lastAssistant = messages.find((m) => m.type === "assistant")
      const replyText = lastAssistant && "content" in lastAssistant
        ? lastAssistant.content
            .filter((c): c is { type: "text"; text: string } => "type" in c && c.type === "text")
            .map((c) => c.text)
            .join("\n")
        : ""

      eventBus.publish(
        AgentEvent.statusUpdate({
          taskId,
          contextId,
          status: { state, message: agentMessage(lastAssistant?.id ?? "unknown", contextId, taskId, replyText), timestamp: new Date().toISOString() } as TaskStatus,
          metadata: undefined,
        }),
      )
    },

    cancelTask: async (taskId, eventBus) => {
      const taskRow = await db.resolveTask(taskId)
      if (!taskRow) return

      const interrupted = await run(session.interrupt(taskRow.sessionID as Session.ID, { resume: false }))
      const state = interrupted ? TaskState.TASK_STATE_CANCELED : TaskState.TASK_STATE_COMPLETED
      eventBus.publish(
        AgentEvent.statusUpdate({
          taskId,
          contextId: taskRow.contextId,
          status: { state, message: undefined, timestamp: new Date().toISOString() } as TaskStatus,
          metadata: undefined,
        }),
      )
    },
  }

  // --- SDK handler (lazy, memoized) ---

  const sdkCache = yield* Ref.make<{ readonly transport: JsonRpcTransportHandler } | null>(null)
  const ensureSdk = Effect.gen(function* () {
    const cached = yield* Ref.get(sdkCache)
    if (cached !== null) return cached
    const builtCard = yield* card()
    const requestHandler = new DefaultRequestHandler(builtCard, new SqliteTaskStore(db), executor)
    const transport = new JsonRpcTransportHandler(requestHandler)
    const entry = { transport }
    yield* Ref.set(sdkCache, entry)
    return entry
  })

  /** JSON-RPC handler. */
  const rpc = Effect.fnUntraced(function* (body: unknown, httpRequest?: unknown) {
    const on = yield* a2aEnabled
    if (!on) return { jsonrpc: "2.0" as const, id: null, error: { code: -32601, message: "A2A is not enabled" } }

    const authResult = yield* authorize(httpRequest).pipe(
      Effect.matchEffect({
        onFailure: (error) => Effect.succeed(Option.some({ jsonrpc: "2.0" as const, id: null, error: { code: -32601, message: error.message } })),
        onSuccess: () => Effect.succeed(Option.none()),
      }),
    )
    if (Option.isSome(authResult)) return authResult.value

    const method = typeof body === "object" && body !== null ? (body as { method?: string }).method : undefined
    if (method === "a2a/listSessions") {
      const dir = typeof body === "object" && body !== null ? (body as { params?: { directory?: string } }).params?.directory : undefined
      const peers = yield* sessions(dir)
      const id = typeof body === "object" && body !== null && "id" in body ? (body as { id: string | number }).id : null
      return { jsonrpc: "2.0" as const, id, result: { peers } }
    }

    const sdk = yield* ensureSdk
    const context = defaultServerCallContextBuilder({ extensions: undefined, user: undefined, headers: {} })
    const result = yield* Effect.tryPromise({
      try: () => sdk.transport.handle(body as Record<string, unknown>, context),
      catch: (cause) => ({ jsonrpc: "2.0" as const, id: null, error: { code: -32603, message: cause instanceof Error ? cause.message : "Internal error" } }),
    })

    if (result && typeof result === "object" && !(Symbol.asyncIterator in (result as object))) return result
    return { jsonrpc: "2.0" as const, id: null, error: { code: -32603, message: "Unexpected streaming response" } }
  })

  /** Streaming handler. */
  const streamMessage = Effect.fn("A2a.streamMessage")(function* (body: unknown, httpRequest?: unknown) {
    const on = yield* a2aEnabled
    if (!on) return Stream.make(`data: ${JSON.stringify({ jsonrpc: "2.0", id: null, error: { code: -32601, message: "A2A is not enabled" } })}\n\n`)

    const authResult = yield* authorize(httpRequest).pipe(
      Effect.matchEffect({
        onFailure: (error) => Effect.succeed(Option.some(`data: ${JSON.stringify({ jsonrpc: "2.0", id: null, error: { code: -32601, message: error.message } })}\n\n`)),
        onSuccess: () => Effect.succeed(Option.none()),
      }),
    )
    if (Option.isSome(authResult)) return Stream.make(authResult.value)

    const sdk = yield* ensureSdk
    const context = defaultServerCallContextBuilder({ extensions: undefined, user: undefined, headers: {} })
    const result = yield* Effect.tryPromise({
      try: () => sdk.transport.handle(body as Record<string, unknown>, context),
      catch: () => null,
    })

    if (!result || typeof result !== "object" || !(Symbol.asyncIterator in (result as object))) {
      const id = typeof body === "object" && body !== null && "id" in body ? (body as { id: string | number }).id : null
      return Stream.make(`data: ${JSON.stringify({ jsonrpc: "2.0", id, error: { code: -32603, message: "Expected streaming response" } })}\n\n`)
    }

    const gen = result as AsyncGenerator<unknown>
    return Stream.fromAsyncIterable(gen, () => void 0).pipe(
      Stream.map((response) => `data: ${JSON.stringify(response)}\n\n`),
    )
  })

  return { card, sessions, rpc, streamMessage }
})
