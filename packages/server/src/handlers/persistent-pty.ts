import { PersistentPty } from "@opencode/core/persistent-pty"
import { PtyTicket } from "@opencode/core/pty/ticket"
import {
  ForbiddenError,
  InvalidRequestError,
  PtyFeatureUnsupportedError,
  PtyNotFoundError,
  ServiceUnavailableError,
} from "@opencode/protocol/errors"
import {
  PTY_CONNECT_TICKET_QUERY,
  PTY_CONNECT_TOKEN_HEADER,
  PTY_CONNECT_TOKEN_HEADER_VALUE,
} from "@opencode/protocol/groups/persistent-pty"
import { Effect, Queue, Stream } from "effect"
import { HttpServerRequest, HttpServerResponse } from "effect/http"
import { HttpApiBuilder, HttpApiSchema } from "effect/http-api"
import { Socket } from "effect/socket"
import { Api } from "../api"
import { CorsConfig, isAllowedRequestOrigin } from "../cors"
import { makeA2a } from "./a2a"
import { makeArtifact } from "./artifact"
import { makeJob } from "./job"
import { makeMediaGet } from "./media"
import { type Outbound, runPtySocket } from "./pty-socket"
import { makeSchedule } from "./schedule"

export const PersistentPtyHandler = HttpApiBuilder.group(Api, "server.experimental", (handlers) =>
  Effect.gen(function* () {
    const tickets = yield* PtyTicket.Service
    const cors = yield* CorsConfig
    const pty = yield* PersistentPty.Service
    const artifact = yield* makeArtifact
    const job = yield* makeJob
    const mediaGet = yield* makeMediaGet
    const schedule = yield* makeSchedule
    const a2a = yield* makeA2a

    return handlers
      .handleRaw("media.get", (ctx) => mediaGet(ctx.params.hash, ctx.request))
      .handle("job.list", (ctx) => job.list(ctx.query))
      .handle("artifacts.providers", (ctx) => artifact.providers(ctx.query, ctx.request))
      .handle("artifacts.list", (ctx) => artifact.list(ctx.query, ctx.request))
      .handle("artifacts.create", (ctx) => artifact.create(ctx.query, ctx.payload, ctx.request))
      .handle("artifacts.get", (ctx) => artifact.get(ctx.params.artifactID, ctx.query, ctx.request))
      .handle("artifacts.update", (ctx) => artifact.update(ctx.params.artifactID, ctx.query, ctx.payload, ctx.request))
      .handle("artifacts.delete", (ctx) => artifact.remove(ctx.params.artifactID, ctx.query, ctx.request))
      .handleRaw("artifacts.view", (ctx) => artifact.view(ctx.params.artifactID, ctx.query, ctx.request))
      .handle("schedule.create", (ctx) => schedule.create(ctx.payload))
      .handle("schedule.list", (ctx) => schedule.list(ctx.query.sessionID))
      .handle("schedule.get", (ctx) => schedule.get(ctx.params.scheduleID))
      .handle("schedule.pause", (ctx) => schedule.pause(ctx.params.scheduleID))
      .handle("schedule.resume", (ctx) => schedule.resume(ctx.params.scheduleID))
      .handle("schedule.delete", (ctx) => schedule.remove(ctx.params.scheduleID))
      .handle("a2a.card", () =>
        a2a
          .card()
          .pipe(Effect.catchTag("A2A.DisabledError", () => Effect.fail(new ForbiddenError({ message: "A2A is not enabled" })))),
      )
      .handle("a2a.sessions", (ctx) =>
        a2a
          .sessions(ctx.query.directory)
          .pipe(
            Effect.map((data) => ({ data })),
            Effect.catchTag("A2A.DisabledError", () => Effect.fail(new ForbiddenError({ message: "A2A is not enabled" }))),
            Effect.catchTag("A2A.UnauthorizedError", (error) => Effect.fail(new ForbiddenError({ message: error.message }))),
            Effect.catchTag("Session.InvalidListCursorError", () => Effect.fail(new InvalidRequestError({ message: "Invalid list cursor" }))),
          ),
      )
      .handleRaw("a2a.rpc", (ctx) =>
        Effect.gen(function* () {
          const body = yield* ctx.request.json.pipe(
            Effect.mapError(() => new InvalidRequestError({ message: "Invalid JSON body" })),
          )

          // Streaming requests get an SSE response
          if (typeof body === "object" && body !== null && (body as { method?: string }).method === "SendStreamingMessage") {
            const frames = yield* a2a.streamMessage(body, ctx.request).pipe(
              Effect.mapError(() => new InvalidRequestError({ message: "A2A streaming failed" })),
            )
            const heartbeat = Stream.tick("15 seconds").pipe(Stream.map(() => ": heartbeat\n\n"))
            const output = frames.pipe(
              Stream.filter((frame: string) => frame !== ""),
              Stream.merge(heartbeat, { haltStrategy: "left" }),
              Stream.encodeText,
            )
            return HttpServerResponse.stream(output, {
              contentType: "text/event-stream",
              headers: {
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
                "X-Content-Type-Options": "nosniff",
              },
            })
          }

          const response = yield* a2a.rpc(body, ctx.request).pipe(
            Effect.mapError(() => new InvalidRequestError({ message: "A2A request failed" })),
          )
          // The response may contain branded types or undefined values that the framework's
          // JSON encoder rejects. Round-trip through JSON to produce plain JSON.
          const jsonText = JSON.stringify(response)
          return HttpServerResponse.text(jsonText, {
            contentType: "application/json",
            headers: { "Cache-Control": "no-cache" },
          })
        }),
      )
      .handle(
        "persistentPty.read",
        Effect.fn(function* (ctx) {
          return { data: yield* pty.read(ctx.params.sessionID, ctx.query.lines).pipe(mapUnavailable) }
        }),
      )
      .handle(
        "persistentPty.list",
        Effect.fn(function* (ctx) {
          return { data: yield* pty.list(ctx.params.sessionID).pipe(mapUnavailable) }
        }),
      )
      .handle(
        "persistentPty.create",
        Effect.fn(function* (ctx) {
          return {
            data: yield* pty
              .create(ctx.params.sessionID, {
                command: ctx.payload.command,
                args: ctx.payload.args,
                cwd: ctx.payload.cwd,
                title: ctx.payload.title,
                env: ctx.payload.env,
                cols: ctx.payload.size?.cols,
                rows: ctx.payload.size?.rows,
              })
              .pipe(
                Effect.catchTags({
                  "PersistentPty.InvalidCwdError": (error) =>
                    new InvalidRequestError({ message: error.message, kind: "invalid_cwd", field: "cwd" }),
                  "PersistentPty.UnavailableError": unavailable,
                }),
              ),
          }
        }),
      )
      .handle(
        "persistentPty.shutdown",
        Effect.fn(function* () {
          yield* pty.shutdown().pipe(mapUnavailable)
          return HttpApiSchema.NoContent.make()
        }),
      )
      .handle(
        "persistentPty.handoff",
        Effect.fn(function* () {
          return { handoff: yield* pty.handoff().pipe(mapUnavailable) }
        }),
      )
      .handle(
        "persistentPty.get",
        Effect.fn(function* (ctx) {
          return { data: yield* pty.get(ctx.params.ptyID).pipe(mapTerminalError) }
        }),
      )
      .handle(
        "persistentPty.update",
        Effect.fn(function* (ctx) {
          if (ctx.payload.size)
            yield* pty
              .resize(ctx.params.ptyID, ctx.payload.size.cols, ctx.payload.size.rows, ctx.payload.attachmentID)
              .pipe(mapTerminalError)
          if (ctx.payload.title === undefined) return { data: yield* pty.get(ctx.params.ptyID).pipe(mapTerminalError) }
          return {
            data: yield* pty.rename(ctx.params.ptyID, ctx.payload.title).pipe(
              Effect.catchTags({
                "PersistentPty.NotFoundError": notFound,
                "PersistentPty.UnsupportedError": (error) =>
                  new PtyFeatureUnsupportedError({
                    feature: error.feature,
                    message: "Terminal renaming will be available after OpenCode's terminal service restarts",
                  }),
                "PersistentPty.UnavailableError": unavailable,
              }),
            ),
          }
        }),
      )
      .handle(
        "persistentPty.snapshot",
        Effect.fn(function* (ctx) {
          return { data: yield* pty.snapshot(ctx.params.ptyID).pipe(mapTerminalError) }
        }),
      )
      .handle(
        "persistentPty.remove",
        Effect.fn(function* (ctx) {
          yield* pty.remove(ctx.params.ptyID).pipe(mapTerminalError)
          return HttpApiSchema.NoContent.make()
        }),
      )
      .handle(
        "persistentPty.connectToken",
        Effect.fn(function* (ctx) {
          const request = yield* HttpServerRequest.HttpServerRequest
          if (
            request.headers[PTY_CONNECT_TOKEN_HEADER] !== PTY_CONNECT_TOKEN_HEADER_VALUE ||
            !isAllowedRequestOrigin(request.headers.origin, request.headers.host, cors)
          )
            return yield* new ForbiddenError({ message: "Invalid persistent PTY connect token request" })
          yield* pty.get(ctx.params.ptyID).pipe(mapTerminalError)
          return { data: yield* tickets.issue({ ptyID: ctx.params.ptyID }) }
        }),
      )
      .handleRaw(
        "persistentPty.connect",
        Effect.fn("PersistentPtyHandler.connect")(function* (ctx) {
          if (!isAllowedRequestOrigin(ctx.request.headers.origin, ctx.request.headers.host, cors))
            return HttpServerResponse.empty({ status: 403 })

          const url = new URL(ctx.request.url, "http://localhost")
          const ticket = url.searchParams.get(PTY_CONNECT_TICKET_QUERY)
          if (ticket && !(yield* tickets.consume({ ticket, ptyID: ctx.params.ptyID })))
            return HttpServerResponse.empty({ status: 403 })

          const cursor = Number(url.searchParams.get("cursor") ?? "0")
          const role = url.searchParams.get("role") === "observer" ? "observer" : "controller"
          const framedInput = url.searchParams.get("input_protocol") === "1"
          const attachmentID = url.searchParams.get("attachment_id") ?? crypto.randomUUID()
          if (!Number.isSafeInteger(cursor) || cursor < 0) return HttpServerResponse.empty({ status: 400 })

          const socket = yield* Effect.orDie(ctx.request.upgrade)
          const outbox = yield* Queue.unbounded<Outbound>()
          let attachment: PersistentPty.Attachment | undefined
          // Bun's native ws upgrade must start before asynchronous daemon I/O.
          const onOpen = Effect.gen(function* () {
            attachment = yield* pty
              .attach(ctx.params.ptyID, {
                cursor,
                attachmentID,
                role,
                takeover: url.searchParams.get("takeover") === "true",
                onEvent: (event) => {
                  if (event.type === "output") Queue.offerUnsafe(outbox, event.data)
                  if (event.type === "resized")
                    Queue.offerUnsafe(
                      outbox,
                      JSON.stringify({ ...event, checkpoint: Buffer.from(event.checkpoint).toString("base64") }),
                    )
                  if (event.type !== "output" && event.type !== "resized")
                    Queue.offerUnsafe(outbox, JSON.stringify(event))
                },
                onEnd: () => Queue.offerUnsafe(outbox, new Socket.CloseEvent(1000)),
              })
              .pipe(
                Effect.catchTags({
                  "PersistentPty.NotFoundError": () => Effect.succeed(undefined),
                  "PersistentPty.UnavailableError": () => Effect.succeed(undefined),
                }),
              )
            if (!attachment) {
              Queue.offerUnsafe(outbox, new Socket.CloseEvent(4404, "terminal unavailable"))
              return
            }

            Queue.offerUnsafe(
              outbox,
              JSON.stringify({
                type: "attached",
                attachmentID,
                inputProtocol: framedInput ? 1 : 0,
                info: attachment.info,
                role: attachment.role,
                generation: attachment.generation,
                replay: {
                  requestedOffset: attachment.replay.requestedOffset,
                  availableOffset: attachment.replay.availableOffset,
                  endOffset: attachment.replay.endOffset,
                  truncated: attachment.replay.truncated,
                },
              }),
            )
            if (attachment.replay.data.length > 0) Queue.offerUnsafe(outbox, attachment.replay.data)
            Queue.offerUnsafe(
              outbox,
              JSON.stringify({ type: "replay_complete", endOffset: attachment.replay.endOffset }),
            )
            attachment.activate()
          })

          yield* runPtySocket({
            socket,
            outbox,
            onOpen,
            onMessage: (message) => {
              if (!attachment) return Effect.void
              const data = typeof message === "string" ? Buffer.from(message) : message
              if (!framedInput)
                return pty
                  .input(ctx.params.ptyID, attachmentID, attachment.info.size.cols, attachment.info.size.rows, data)
                  .pipe(Effect.ignore)
              if (data.byteLength < 5) return Effect.void
              const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
              const type = data[0]
              const cols = view.getUint16(1)
              const rows = view.getUint16(3)
              if ((type !== 0 && type !== 1) || cols === 0 || rows === 0) return Effect.void
              if (type === 0) return pty.control(ctx.params.ptyID, attachmentID, cols, rows).pipe(Effect.ignore)
              return pty.input(ctx.params.ptyID, attachmentID, cols, rows, data.subarray(5)).pipe(Effect.ignore)
            },
            detach: () => attachment?.detach(),
          })
          return HttpServerResponse.empty()
        }),
      )
  }),
)

const mapUnavailable = <A>(effect: Effect.Effect<A, PersistentPty.UnavailableError>) =>
  effect.pipe(Effect.catchTag("PersistentPty.UnavailableError", unavailable))

const mapTerminalError = <A>(effect: Effect.Effect<A, PersistentPty.NotFoundError | PersistentPty.UnavailableError>) =>
  effect.pipe(
    Effect.catchTags({
      "PersistentPty.NotFoundError": notFound,
      "PersistentPty.UnavailableError": unavailable,
    }),
  )

const notFound = (error: PersistentPty.NotFoundError) =>
  new PtyNotFoundError({ ptyID: error.ptyID, message: `PTY session not found: ${error.ptyID}` })

const unavailable = (error: PersistentPty.UnavailableError) =>
  new ServiceUnavailableError({ message: error.message, service: "opencode-pty" })
