import os from "node:os"
import path from "node:path"

// Server tests boot the full application stack (config discovery, provider policies,
// plugins, a2a.db), so a developer's personal global config must not leak in. CI starts
// from a clean home already; this pins local runs to the same state.
const root = path.join(os.tmpdir(), "opencode-server-test")
process.env.XDG_CONFIG_HOME = path.join(root, "config")
process.env.XDG_DATA_HOME = path.join(root, "data")
process.env.XDG_STATE_HOME = path.join(root, "state")
process.env.XDG_CACHE_HOME = path.join(root, "cache")
process.env.OPENCODE_TEST_HOME = path.join(root, "home")
