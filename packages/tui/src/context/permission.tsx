import { createSignal } from "solid-js"
import { useConfig } from "../config"
import { useArgs } from "./args"
import { createSimpleContext } from "./helper"

export type PermissionMode = "prompt" | "autoaccept"

export const { use: usePermission, provider: PermissionProvider } = createSimpleContext({
  name: "Permission",
  init: () => {
    const args = useArgs()
    const config = useConfig()
    const initial = args.auto ? "autoaccept" : config.data.session.permissions
    const [mode, setMode] = createSignal<PermissionMode>(initial)
    return {
      get mode(): PermissionMode {
        return mode()
      },
      setMode,
      toggle() {
        setMode((prev) => (prev === "autoaccept" ? "prompt" : "autoaccept"))
      },
    }
  },
})
