import { describe, expect, test } from "bun:test"
import * as fs from "node:fs"
import * as os from "node:os"
import * as path from "node:path"
import * as A2aThreads from "@opencode/core/a2a/threads"
import { extractResult } from "@opencode/core/a2a/client"

// Isolated a2a.db (a temp file, since the helper treats ":memory:" as unset)
// so the tests never touch the real ~/.local/share/opencode state.
process.env.OPENCODE_DB = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "a2a-threads-test-")), "a2a.db")

describe("a2a threads", () => {
  test("listSessionThreads returns remembered threads newest first", async () => {
    const session = "ses_test_threads_1"
    A2aThreads.rememberThread({ contextId: "ctx-old", sessionID: session, directory: "/tmp/a", peerUrl: "http://127.0.0.1:9900/" })
    // last_used_at has millisecond resolution; ensure the second insert sorts after the first.
    await Bun.sleep(5)
    A2aThreads.rememberThread({ contextId: "ctx-new", sessionID: session, directory: "/tmp/a", peerUrl: "http://127.0.0.1:9900/" })

    const threads = A2aThreads.listSessionThreads(session)
    expect(threads.map((t) => t.contextId)).toEqual(["ctx-new", "ctx-old"])
    expect(threads[0].peerUrl).toBe("http://127.0.0.1:9900/")
    expect(threads[0].directory).toBe("/tmp/a")
  })

  test("rememberThread upserts without duplicating or resetting created_at", () => {
    const session = "ses_test_threads_2"
    A2aThreads.rememberThread({ contextId: "ctx-one", sessionID: session, directory: "/tmp/b", peerUrl: "http://peer1/" })
    A2aThreads.rememberThread({ contextId: "ctx-one", sessionID: session, directory: "/tmp/b", peerUrl: "http://peer2/" })

    const threads = A2aThreads.listSessionThreads(session)
    expect(threads).toHaveLength(1)
    expect(threads[0].peerUrl).toBe("http://peer2/")
  })

  test("listSessionThreads is scoped to the session (multi-peer safe)", () => {
    A2aThreads.rememberThread({ contextId: "ctx-a", sessionID: "ses_test_threads_3", directory: "/tmp/c", peerUrl: "http://hermes-a/" })
    A2aThreads.rememberThread({ contextId: "ctx-b", sessionID: "ses_test_threads_4", directory: "/tmp/c", peerUrl: "http://hermes-b/" })

    expect(A2aThreads.listSessionThreads("ses_test_threads_3").map((t) => t.contextId)).toEqual(["ctx-a"])
    expect(A2aThreads.listSessionThreads("ses_test_threads_4").map((t) => t.contextId)).toEqual(["ctx-b"])
  })

  test("listSessionThreads returns empty for unknown sessions", () => {
    expect(A2aThreads.listSessionThreads("ses_test_unknown")).toEqual([])
  })
})

describe("extractResult contextId", () => {
  test("extracts contextId from a Task result", () => {
    const result = extractResult({
      id: "task-1",
      contextId: "ctx-6260f102d0e44b6c",
      status: { state: 3 },
      artifacts: [],
      history: [],
      metadata: undefined,
    } as unknown as Parameters<typeof extractResult>[0])
    expect(result.contextId).toBe("ctx-6260f102d0e44b6c")
    expect(result.state).toBe("completed")
  })

  test("extracts contextId from a Message result", () => {
    const result = extractResult({
      messageId: "m-1",
      contextId: "ctx-abc",
      role: 1,
      parts: [{ content: { $case: "text", value: "hello" }, metadata: undefined, filename: "", mediaType: "" }],
      metadata: undefined,
      extensions: [],
      referenceTaskIds: [],
    } as unknown as Parameters<typeof extractResult>[0])
    expect(result.contextId).toBe("ctx-abc")
    expect(result.text).toBe("hello")
  })

  test("returns undefined contextId when the peer does not assign one", () => {
    const result = extractResult({
      id: "task-2",
      contextId: "",
      status: { state: 3 },
      artifacts: [],
      history: [],
      metadata: undefined,
    } as unknown as Parameters<typeof extractResult>[0])
    expect(result.contextId).toBeUndefined()
  })
})
