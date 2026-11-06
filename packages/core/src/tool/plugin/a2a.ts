export * as A2aTools from "./a2a.js"

import type { Context } from "@opencode/plugin/effect/plugin"
import { ToolFailure } from "@opencode/ai/effect"
import { optional } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import { Effect, Schema } from "effect"
import { Config } from "../../config.js"
import { ConfigEntryObserver } from "../../config/plugin/entry-observer.js"
import { Flag } from "../../flag.js"
import PROMPT_A2A from "../../plugin/command/a2a.txt"
import { Permission } from "../../permission.js"
import { Session as SessionService } from "../../session.js"
import { A2aClient } from "../../a2a/client.js"

/** Validate that a URL is http(s) and has no embedded credentials.
 *  SSRF defense: reject non-http(s) protocols and URL-embedded credentials. */
function assertHttpUrl(url: string): string {
  const parsed = new URL(url)
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:")
    throw new Error("URL must use http:// or https://")
  if (parsed.username || parsed.password)
    throw new Error("URL must not include credentials")
  return parsed.href
}

/** A peer as the model sees it in a2a_peers output. */
const Peer = Schema.Struct({
  sessionID: Session.ID,
  title: Schema.String.pipe(optional),
})

const PeersOutput = Schema.Struct({
  peers: Schema.Array(Peer),
})

const CallOutput = Schema.Struct({
  state: Schema.Literals(["completed", "failed", "canceled", "input-required", "working"]),
  text: Schema.String,
  taskId: Schema.String.pipe(optional).annotate({
    description: "The A2A task ID. Present when the task is still working (async mode) for later polling with a2a_get_task.",
  }),
})

const GetTaskOutput = Schema.Struct({
  state: Schema.Literals(["completed", "failed", "canceled", "input-required", "working"]),
  text: Schema.String,
  taskId: Schema.String,
})

const CancelTaskOutput = Schema.Struct({
  state: Schema.Literals(["completed", "failed", "canceled", "input-required", "working"]),
  text: Schema.String,
  taskId: Schema.String,
})

const DiscoverOutput = Schema.Struct({
  name: Schema.String,
  description: Schema.String.pipe(optional),
  url: Schema.String,
  skills: Schema.Array(Schema.String).pipe(optional),
})

export const Plugin = {
  id: "opencode.tool.a2a",
  effect: Effect.fn("A2aTools.Plugin")(function* (ctx: Context) {
    const config = yield* Config.Service
    const session = yield* SessionService.Service
    const permission = yield* Permission.Service
    const loaded = yield* ConfigEntryObserver.observe(config, ctx.event, ctx.tool.reload())
    const flags = yield* Flag.resolver
    const enabled = () => flags(loaded.entries).a2a.enabled
    const failure = (error: unknown) =>
      new ToolFailure({
        message: error instanceof Error ? error.message : "A2A tool failed",
        error,
      })

    yield* ctx.tool
      .transform((editor) => {
        if (!enabled()) return

        editor.add({
          name: "a2a_peers",
          description:
            "List other opencode sessions in the same working directory that can receive A2A messages. Use this to discover peers before calling a2a_call.",
          input: Schema.Struct({}),
          output: PeersOutput,
          options: { namespace: "opencode", codemode: true },
          execute: (_, context) =>
            Effect.gen(function* () {
              const info = yield* session.get(context.sessionID)
              const result = yield* session.list({ directory: info.location.directory })
              const peers = result.data
                .filter((s) => s.id !== context.sessionID && s.time.archived === undefined)
                .map((s) => ({ sessionID: s.id, title: s.title }))
              return {
                output: { peers },
                content: peers.length === 0
                  ? "No peers found."
                  : peers.map((p) => `${p.sessionID}${p.title ? ` (${p.title})` : ""}`).join("\n"),
              }
            }).pipe(Effect.mapError(failure)),
        })

        editor.add({
          name: "a2a_call",
          description:
            "Send a message to another A2A agent. For a local opencode session, pass its session ID from a2a_peers. For any remote A2A-compatible agent (Hermes, Google ADK, etc.), pass its URL. The peer processes the message and returns its reply. Set async:true to return immediately with a task ID when the peer is still working — poll later with a2a_get_task. The A2A_HERMES_TOKEN env var is used automatically when no auth is passed.",
          input: Schema.Struct({
            sessionID: Session.ID.pipe(optional).annotate({
              description: "A local opencode session ID to message (from a2a_peers).",
            }),
            url: Schema.String.pipe(optional).annotate({
              description: "A remote A2A endpoint URL. Only used when sessionID is not provided. For local opencode peers, use sessionID from a2a_peers instead.",
            }),
            message: Schema.String.annotate({ description: "The text to send to the peer." }),
            auth: Schema.String.pipe(optional).annotate({
              description: "Optional bearer token for the remote peer. Defaults to A2A_HERMES_TOKEN from the environment.",
            }),
            contextId: Schema.String.pipe(optional).annotate({
              description: "A2A context ID for multi-turn conversations with the peer. Reusing the same contextId lets the peer maintain conversation state across calls.",
            }),
            async: Schema.Boolean.pipe(optional).annotate({
              description: "When true, return immediately with a task ID if the peer task is still working, instead of blocking until completion. Use a2a_get_task to poll for the result.",
            }),
          }),
          output: CallOutput,
          options: { namespace: "opencode", codemode: true },
          execute: (input, context) =>
            Effect.gen(function* () {
              if (!input.url && !input.sessionID)
                return { output: { state: "failed" as const, text: "Provide either url or sessionID." }, content: "Provide either url or sessionID." }

              // Build metadata so the peer knows where the call came from.
              const callerInfo = yield* Effect.gen(function* () {
                return yield* session.get(context.sessionID).pipe(
                  Effect.map((info) => ({ title: info.title, directory: info.location.directory })),
                  Effect.catchTag("Session.NotFoundError", () => Effect.succeed({ title: undefined, directory: undefined })),
                )
              })
              const a2aMetadata: Record<string, unknown> = {
                opencode: {
                  sessionID: context.sessionID,
                  title: callerInfo.title,
                  directory: callerInfo.directory,
                },
              }

              // Remote peer: use the A2A HTTP client (only when no sessionID is given)
              if (input.url && !input.sessionID) {
                const url = yield* Effect.try({
                  try: () => assertHttpUrl(input.url!),
                  catch: (error) => error,
                })
                yield* permission.assert({
                  action: "webfetch",
                  resources: [url],
                  save: ["*"],
                  metadata: input,
                  ...Permission.invocation(context),
                })

                // Async mode: use streaming to return early with a task ID.
                if (input.async) {
                  const asyncResult = yield* A2aClient.sendMessageAsync({ url, message: input.message, auth: input.auth, contextId: input.contextId, metadata: a2aMetadata })
                  if (asyncResult.state === "working" && asyncResult.taskId) {
                    return {
                      output: { state: "working" as const, text: "", taskId: asyncResult.taskId },
                      content: `Task ${asyncResult.taskId} is still working. Poll with a2a_get_task to get the result.`,
                    }
                  }
                  return { output: { state: asyncResult.state as "completed" | "failed" | "canceled" | "input-required" | "working", text: asyncResult.text, taskId: asyncResult.taskId || undefined }, content: asyncResult.text || `Task ${asyncResult.state}` }
                }

                const result = yield* A2aClient.sendMessage({ url, message: input.message, auth: input.auth, contextId: input.contextId, metadata: a2aMetadata })
                const { state, text, taskId } = A2aClient.extractResult(result)
                return { output: { state: state as "completed" | "failed" | "canceled" | "input-required" | "working", text, taskId }, content: text || `Task ${state}` }
              }

              // Local peer: use Session.prompt directly (no HTTP round-trip)
              const targetID = input.sessionID!
              yield* session.prompt({
                sessionID: targetID,
                text: input.message,
                delivery: "steer",
                metadata: { a2a: { from: context.sessionID, caller: a2aMetadata.opencode } },
              })

              // Wait for the target session to finish working
              yield* session.wait(targetID).pipe(Effect.ignore)

              // Read the last assistant message
              const messages = yield* session.messages({ sessionID: targetID, limit: 10, order: "desc" as const })
              const lastAssistant = messages.find((m) => m.type === "assistant")
              const text = lastAssistant && "content" in lastAssistant
                ? lastAssistant.content
                    .filter((c): c is { type: "text"; text: string } => "type" in c && c.type === "text")
                    .map((c) => c.text)
                    .join("\n")
                : ""

              return {
                output: { state: "completed" as const, text },
                content: text || "No reply.",
              }
            }).pipe(Effect.mapError(failure)),
        })

        editor.add({
          name: "a2a_get_task",
          description:
            "Poll a remote A2A peer for the status of a task started by a2a_call with async:true. Returns the task state and any reply text available so far. The A2A_HERMES_TOKEN env var is used automatically when no auth is passed.",
          input: Schema.Struct({
            url: Schema.String.annotate({ description: "The remote A2A endpoint URL." }),
            taskId: Schema.String.annotate({ description: "The task ID returned by a2a_call in async mode." }),
            auth: Schema.String.pipe(optional).annotate({
              description: "Optional bearer token. Defaults to A2A_HERMES_TOKEN from the environment.",
            }),
            historyLength: Schema.Number.pipe(optional).annotate({
              description: "Maximum number of recent messages to retrieve from the task history. 0 means none.",
            }),
          }),
          output: GetTaskOutput,
          options: { namespace: "opencode", codemode: true },
          execute: (input, context) =>
            Effect.gen(function* () {
              const url = yield* Effect.try({
                try: () => assertHttpUrl(input.url),
                catch: (error) => error,
              })
              yield* permission.assert({
                action: "webfetch",
                resources: [url],
                save: ["*"],
                metadata: input,
                ...Permission.invocation(context),
              })

              const { state, text, taskId } = yield* A2aClient.getTask({ url, taskId: input.taskId, auth: input.auth, historyLength: input.historyLength })
              return {
                output: { state: state as "completed" | "failed" | "canceled" | "input-required" | "working", text, taskId },
                content: text || `Task ${taskId} is ${state}.`,
              }
            }).pipe(Effect.mapError(failure)),
        })

        editor.add({
          name: "a2a_cancel_task",
          description:
            "Cancel a task on a remote A2A peer. The task must have been started by a2a_call. Returns the updated task state. The A2A_HERMES_TOKEN env var is used automatically when no auth is passed.",
          input: Schema.Struct({
            url: Schema.String.annotate({ description: "The remote A2A endpoint URL." }),
            taskId: Schema.String.annotate({ description: "The task ID to cancel." }),
            auth: Schema.String.pipe(optional).annotate({
              description: "Optional bearer token. Defaults to A2A_HERMES_TOKEN from the environment.",
            }),
          }),
          output: CancelTaskOutput,
          options: { namespace: "opencode", codemode: true },
          execute: (input, context) =>
            Effect.gen(function* () {
              const url = yield* Effect.try({
                try: () => assertHttpUrl(input.url),
                catch: (error) => error,
              })
              yield* permission.assert({
                action: "webfetch",
                resources: [url],
                save: ["*"],
                metadata: input,
                ...Permission.invocation(context),
              })

              const { state, text, taskId } = yield* A2aClient.cancelTask({ url, taskId: input.taskId, auth: input.auth })
              return {
                output: { state: state as "completed" | "failed" | "canceled" | "input-required" | "working", text, taskId },
                content: text || `Task ${taskId} canceled.`,
              }
            }).pipe(Effect.mapError(failure)),
        })

        editor.add({
          name: "a2a_discover",
          description:
            "Fetch and summarize a remote A2A agent's Agent Card to learn its name, capabilities, and endpoint URL before calling a2a_call.",
          input: Schema.Struct({
            url: Schema.String.annotate({ description: "The base URL of the remote A2A agent." }),
          }),
          output: DiscoverOutput,
          options: { namespace: "opencode", codemode: true },
          execute: (input, context) =>
            Effect.gen(function* () {
              const url = yield* Effect.try({
                try: () => assertHttpUrl(input.url),
                catch: (error) => error,
              })
              yield* permission.assert({
                action: "webfetch",
                resources: [url],
                save: ["*"],
                metadata: { url: input.url },
                ...Permission.invocation(context),
              })
              const card = yield* A2aClient.fetchCard(url)
              return {
                output: {
                  name: card.name,
                  description: card.description,
                  url: card.url,
                  skills: card.skills,
                },
                content: `${card.name}: ${card.description ?? "A2A agent"}\nURL: ${card.url}\nSkills: ${card.skills?.join(", ") ?? "none"}`,
              }
            }).pipe(Effect.mapError(failure)),
        })
      })
      .pipe(Effect.orDie)

    yield* ctx.command.transform((editor) => {
      if (!enabled()) return
      editor.add({
        name: "a2a",
        description: "send a message to another A2A agent, or list local peers",
        execute: (input) =>
          ctx.session
            .prompt({
              ...input.prompt,
              sessionID: input.sessionID,
              text: PROMPT_A2A.replaceAll("$ARGUMENTS", () => input.prompt.text.trim()),
              delivery: input.delivery,
            })
            .pipe(Effect.asVoid),
      })
    })
  }),
}
