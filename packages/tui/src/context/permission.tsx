import { onCleanup } from "solid-js"
import { createStore, produce } from "solid-js/store"
import { useConfig } from "../config"
import { useArgs } from "./args"
import { useEvent } from "./event"
import { useRoute } from "./route"
import { createSimpleContext } from "./helper"

export type PermissionMode = "prompt" | "autoaccept"

export const { use: usePermission, provider: PermissionProvider } = createSimpleContext({
  name: "Permission",
  init: () => {
    const args = useArgs()
    const config = useConfig()
    const route = useRoute()
    const event = useEvent()
    // Per-session overrides; a session with no entry falls back to the config/`--auto` default.
    // In-memory only: every tab reverts to the default on relaunch.
    const initial: PermissionMode = args.auto ? "autoaccept" : config.data.session.permissions
    const [overrides, setOverrides] = createStore<Record<string, PermissionMode>>({})

    // "dummy" is the --continue boot placeholder (see app.tsx) before the real session resolves;
    // keying under it would lose the toggle and leak. session-tabs.tsx guards the same way.
    function sessionID() {
      const data = route.data
      if (data.type !== "session") return
      if (data.sessionID === "dummy") return
      return data.sessionID
    }

    onCleanup(
      event.on("session.deleted", (evt) => {
        setOverrides(
          produce((draft) => {
            delete draft[evt.data.sessionID]
          }),
        )
      }),
    )

    return {
      get mode(): PermissionMode {
        const id = sessionID()
        return id ? (overrides[id] ?? initial) : initial
      },
      setMode(next: PermissionMode) {
        const id = sessionID()
        if (!id) return
        setOverrides(id, next)
      },
      toggle() {
        const id = sessionID()
        if (!id) return
        const current = overrides[id] ?? initial
        setOverrides(id, current === "autoaccept" ? "prompt" : "autoaccept")
      },
    }
  },
})
