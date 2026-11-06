import { Session } from "@opencode/schema/session"
import { Schema } from "effect"
import { HttpApiEndpoint, OpenApi } from "effect/http-api"
import { ForbiddenError, InvalidRequestError, SessionNotFoundError } from "../errors.js"

const EXPERIMENTAL = "Experimental: needs experimental.a2a in the config and may change without notice."

const annotations = (identifier: string, summary: string, description: string) =>
  OpenApi.annotations({
    identifier: `experimental.a2a.${identifier}`,
    summary,
    description: `${description} ${EXPERIMENTAL}`,
    transform: (operation) => ({ ...operation, tags: ["experimental"] }),
  })

/** Query params for listing sessions by working directory. */
const ListSessionsQuery = Schema.Struct({
  directory: Schema.optional(Schema.String),
})

/** Response shape for session listing. */
const SessionPeer = Schema.Struct({
  sessionID: Session.ID,
  title: Schema.String.pipe(Schema.optional),
  a2aUrl: Schema.String,
  state: Schema.Literals(["idle", "working", "archived"]),
})

/** Lives in the `server.experimental` group (see `groups/experimental.ts`). */
export const A2aEndpoints = [
  // Agent Card — standard A2A discovery path.
  // Not under /api/experimental, so the identifier must not start with "experimental."
  // per the experimental-identifiers test.
  HttpApiEndpoint.get("a2a.card", "/.well-known/agent-card.json", {
    success: Schema.Unknown,
    error: [ForbiddenError],
  }).annotateMerge(
    OpenApi.annotations({
      identifier: "a2a.card",
      summary: "A2A Agent Card",
      description: "The A2A Agent Card advertising this opencode server's capabilities.",
      transform: (operation) => ({ ...operation, tags: ["a2a"] }),
    }),
  ),
  // JSON-RPC 2.0 dispatcher — one endpoint, method-routed
  HttpApiEndpoint.post("a2a.rpc", "/api/experimental/a2a", {
    payload: Schema.Unknown,
    success: Schema.Unknown,
    error: [InvalidRequestError, SessionNotFoundError, ForbiddenError],
  }).annotateMerge(
    annotations(
      "rpc",
      "A2A JSON-RPC endpoint",
      "Dispatches A2A JSON-RPC 2.0 methods: SendMessage, SendStreamingMessage, GetTask, CancelTask, ListTasks, a2a/listSessions.",
    ),
  ),
  // Custom extension: list local sessions by working directory
  HttpApiEndpoint.get("a2a.sessions", "/api/experimental/a2a/sessions", {
    query: ListSessionsQuery,
    success: Schema.Struct({ data: Schema.Array(SessionPeer) }),
    error: [ForbiddenError],
  }).annotateMerge(
    annotations(
      "sessions",
      "List local A2A sessions",
      "List opencode sessions by working directory so peers can discover each other. Custom A2A extension.",
    ),
  ),
] as const
