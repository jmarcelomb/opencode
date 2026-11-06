export * as ConfigExperimental from "./experimental.js"

import { Record, Schema } from "effect"
import { NonNegativeInt, optional } from "../schema.js"
import { ConfigFlag } from "./flag.js"
import { ConfigModel } from "./model.js"
import { ConfigPolicy } from "./policy.js"

export class Info extends Schema.Class<Info>("ConfigExperimental.Info")({
  portable_shell_scanner: Schema.Boolean.pipe(optional).annotate({
    description: "Deprecated: the built-in shell permission scanner is now always enabled.",
  }),
  all_flags: Schema.Boolean.pipe(optional).annotate({
    description:
      "Turn every feature flag on regardless of the build channel, like OPENCODE_FLAGS=all; explicit per-flag settings still win, and false is not a kill switch.",
  }),
  ...Record.map(ConfigFlag.registry, (description) =>
    Schema.Boolean.pipe(optional).annotate({
      description: `${description} Defaults to on in local and dev builds and off in releases.`,
    }),
  ),
  part_rows: Schema.Boolean.pipe(optional).annotate({
    description:
      "Write each assistant part as its own session_part row instead of rewriting the whole message on every event. Defaults to false.",
  }),
  image_generate: Schema.Union([
    Schema.Boolean,
    Schema.Struct({
      model: ConfigModel.Selection.pipe(optional).annotate({
        description:
          "Default model for the experimental_image_generate tool when the agent doesn't choose one, as providerID/modelID. Falls back to automatic selection when it isn't available.",
      }),
    }),
  ])
    .pipe(optional)
    .annotate({
      description:
        "Settings for the experimental_image_generate tool, which is offered while a connected provider has an image model.",
    }),
  subagent_depth: NonNegativeInt.pipe(optional).annotate({
    description: "Maximum subagent nesting depth. Defaults to 1.",
  }),
  location_idle_timeout: Schema.Union([Schema.DurationFromString, Schema.Literal(false)])
    .pipe(optional)
    .annotate({
      description:
        'How long a location can go without session activity before its services are unloaded. Running sessions and background jobs keep a location loaded. Set to false to never unload (default: "60 minutes")',
    }),
  policies: ConfigPolicy.Info.pipe(Schema.Array, optional).annotate({
    description: "Deprecated: use top-level policies. Ordered policies controlling access to configured resources",
  }),
  a2a: Schema.Union([
    Schema.Boolean,
    Schema.Struct({
      tokens: Schema.Array(Schema.String).pipe(optional).annotate({
        description:
          "Bearer tokens accepted by the inbound A2A endpoint. If empty, the endpoint accepts loopback requests only.",
      }),
      peers: Schema.Array(
        Schema.Struct({
          url: Schema.String,
          token: Schema.String.pipe(optional),
          name: Schema.String.pipe(optional),
        }),
      ).pipe(optional).annotate({
        description: "Known external A2A peers for the a2a_discover and a2a_call tools.",
      }),
      directory: Schema.String.pipe(optional).annotate({
        description:
          "Working directory for sessions created by inbound A2A requests. Defaults to the server's process.cwd(). Peers can override per-message via metadata.opencode.directory.",
      }),
      timeout: Schema.Union([Schema.DurationFromString, Schema.Literal(false)]).pipe(optional).annotate({
        description:
          "Maximum time to wait for an inbound A2A task to complete before returning a failed state. Set to false to disable the timeout. Defaults to 10 minutes.",
      }),
    }),
  ])
    .pipe(optional)
    .annotate({
      description:
        "A2A (Agent2Agent) protocol support. Experimental: may change without notice. Set to true to enable with defaults, or provide a struct for tokens and peer configuration.",
    }),
}) {}
