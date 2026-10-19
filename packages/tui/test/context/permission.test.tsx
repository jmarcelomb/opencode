import { expect, test } from "bun:test"
import { renderLocal } from "../fixture/local"

test("permission mode is keyed by sessionID and independent per tab", async () => {
  await using setup = await renderLocal()
  const { local, route } = setup

  // Home route (no session): mode falls back to the config default ("prompt"),
  // and toggle/setMode are no-ops so nothing is keyed against a phantom id.
  expect(route.data.type).toBe("home")
  expect(local.permission.mode).toBe("prompt")
  local.permission.toggle()
  expect(local.permission.mode).toBe("prompt")
  local.permission.setMode("autoaccept")
  expect(local.permission.mode).toBe("prompt")

  // First session starts at the default, then toggles independently.
  route.navigate({ type: "session", sessionID: "ses_a" })
  expect(local.permission.mode).toBe("prompt")
  local.permission.toggle()
  expect(local.permission.mode).toBe("autoaccept")
  local.permission.setMode("prompt")
  expect(local.permission.mode).toBe("prompt")
  local.permission.toggle()
  expect(local.permission.mode).toBe("autoaccept")

  // Second session is untouched by the first and starts at the default.
  route.navigate({ type: "session", sessionID: "ses_b" })
  expect(local.permission.mode).toBe("prompt")
  local.permission.toggle()
  expect(local.permission.mode).toBe("autoaccept")

  // Switching back restores the first session's override.
  route.navigate({ type: "session", sessionID: "ses_a" })
  expect(local.permission.mode).toBe("autoaccept")

  // Going home reverts to the default without losing per-session overrides.
  route.navigate({ type: "home" })
  expect(local.permission.mode).toBe("prompt")
  route.navigate({ type: "session", sessionID: "ses_b" })
  expect(local.permission.mode).toBe("autoaccept")
})

test("the --continue boot \"dummy\" sessionID is not keyed", async () => {
  await using setup = await renderLocal({ args: { auto: true } })
  const { local, route } = setup

  // `--auto` seeds the default so the boot placeholder would read autoaccept,
  // but toggling it must not persist under the literal "dummy" id.
  route.navigate({ type: "session", sessionID: "dummy" })
  expect(local.permission.mode).toBe("autoaccept")
  local.permission.toggle()
  expect(local.permission.mode).toBe("autoaccept")

  // Navigating to a real session starts fresh from the default, proving
  // the dummy toggle did not leak into a shared state.
  route.navigate({ type: "session", sessionID: "ses_real" })
  expect(local.permission.mode).toBe("autoaccept")
  local.permission.toggle()
  expect(local.permission.mode).toBe("prompt")
  route.navigate({ type: "session", sessionID: "ses_other" })
  expect(local.permission.mode).toBe("autoaccept")
})

test("session.deleted drops the override so the session reverts to the default", async () => {
  await using setup = await renderLocal()
  const { local, route, events } = setup

  route.navigate({ type: "session", sessionID: "ses_a" })
  local.permission.toggle()
  expect(local.permission.mode).toBe("autoaccept")

  events.emit({
    id: "evt_deleted",
    created: Date.now(),
    type: "session.deleted",
    durable: { aggregateID: "ses_a", seq: 1, version: 2 },
    location: { directory: "/tmp" },
    data: { sessionID: "ses_a" },
  })

  // The event is delivered through the SSE stream, so wait for the cleanup
  // to land before re-entering the session.
  await setup.waitFor(async () => {
    await Bun.sleep(10)
    route.navigate({ type: "home" })
    route.navigate({ type: "session", sessionID: "ses_a" })
    return local.permission.mode === "prompt"
  })
  expect(local.permission.mode).toBe("prompt")
})
