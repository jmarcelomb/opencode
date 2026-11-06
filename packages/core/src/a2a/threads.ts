/**
 * Direct sqlite access to a2a.db thread state for the a2a_call tool.
 *
 * The server owns a2a.db through the A2aDbLayer (contextId→sessionID mapping
 * for INBOUND A2A messages). The tool side needs only two operations — list
 * the A2A threads associated with the current session, and remember the
 * context/peer of an outbound call — and a small dedicated connection in WAL
 * mode avoids threading server services into the tool plugin context.
 *
 * Threads are (peer_url, contextId) pairs. Multiple peers can hold threads on
 * the same session; `listSessionThreads` returns them all so a2a_call can
 * auto-default to the single-thread case and refuse to guess when ambiguous.
 */
import path from "node:path"

export interface A2aThread {
  readonly contextId: string
  readonly directory: string
  readonly peerUrl: string | undefined
  readonly lastUsedAt: number
}

function resolveDataDir(): string {
  const envDb = process.env.OPENCODE_DB
  if (envDb && envDb !== ":memory:") return path.dirname(envDb)
  const home = process.env.HOME ?? process.env.USERPROFILE ?? "."
  const xdg = process.env.XDG_DATA_HOME
  return xdg ?? path.join(home, ".local", "share")
}

let sqlite: import("bun:sqlite").Database | undefined

function db(): import("bun:sqlite").Database {
  if (sqlite) return sqlite
  const { Database } = require("bun:sqlite")
  const file = path.join(resolveDataDir(), "opencode", "a2a.db")
  require("node:fs").mkdirSync(path.dirname(file), { recursive: true })
  const conn = new Database(file, { create: true })
  conn.exec("PRAGMA journal_mode = WAL")
  conn.exec("PRAGMA synchronous = NORMAL")
  // Mirror the server's schema so the helper works on a fresh data dir too
  // (tests, or opencode running before the server ever created the DB).
  conn.exec(`CREATE TABLE IF NOT EXISTS a2a_context (
    context_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    directory TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    last_used_at INTEGER NOT NULL DEFAULT 0,
    peer_url TEXT
  )`)
  // Existing DBs predate peer_url; the server layer runs the same migration.
  const hasPeerUrl = (
    conn.prepare("SELECT COUNT(*) AS n FROM pragma_table_info('a2a_context') WHERE name = 'peer_url'").get() as { n: number }
  ).n > 0
  if (!hasPeerUrl) conn.exec("ALTER TABLE a2a_context ADD COLUMN peer_url TEXT")
  sqlite = conn
  return conn
}

/** All A2A threads (inbound and outbound) known for a session, newest first. */
export function listSessionThreads(sessionID: string): A2aThread[] {
  const rows = db()
    .prepare(
      "SELECT context_id, directory, peer_url, last_used_at FROM a2a_context WHERE session_id = ? ORDER BY last_used_at DESC, created_at DESC",
    )
    .all(sessionID) as { context_id: string; directory: string; peer_url: string | null; last_used_at: number }[]
  return rows.map((r) => ({
    contextId: r.context_id,
    directory: r.directory,
    peerUrl: r.peer_url ?? undefined,
    lastUsedAt: r.last_used_at,
  }))
}

/** Upsert a thread row for an outbound call, preserving created_at. */
export function rememberThread(input: {
  contextId: string
  sessionID: string
  directory: string
  peerUrl: string
}): void {
  db()
    .prepare(
      `INSERT INTO a2a_context (context_id, session_id, directory, created_at, last_used_at, peer_url)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(context_id) DO UPDATE SET
         session_id = excluded.session_id,
         directory = excluded.directory,
         last_used_at = excluded.last_used_at,
         peer_url = excluded.peer_url`,
    )
    .run(input.contextId, input.sessionID, input.directory, Date.now(), Date.now(), input.peerUrl)
}
