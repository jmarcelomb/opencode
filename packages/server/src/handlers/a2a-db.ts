/**
 * Dedicated SQLite database for A2A state. Isolated from OpenCode's main database
 * so A2A state survives independently and doesn't couple to core's schema migrations.
 *
 * Stores:
 * - contextId → sessionID mapping (persistent sessions across restarts)
 * - task state (taskId → serialized Task, for GetTask across restarts)
 * - task→session mapping (for CancelTask)
 *
 * Session cleanup: contexts with no activity for `SESSION_TTL_MS` are archived
 * on access. Task rows older than `TASK_TTL_MS` are pruned periodically.
 *
 * The main database is touched only through the public Session.Service API.
 */
import { Context, Layer } from "effect"
import path from "node:path"

// Cleanup defaults. Sessions unused for 24h get archived; tasks older than 7d get pruned.
const SESSION_TTL_MS = 24 * 60 * 60 * 1000
const TASK_TTL_MS = 7 * 24 * 60 * 60 * 1000

export interface A2aContextRow {
  readonly contextId: string
  readonly sessionID: string
  readonly directory: string
  readonly createdAt: number
}

export interface A2aTaskRow {
  readonly taskId: string
  readonly contextId: string
  readonly sessionID: string
  readonly createdAt: number
}

export interface A2aDb {
  readonly resolveContext: (contextId: string) => Promise<A2aContextRow | undefined>
  readonly saveContext: (row: A2aContextRow) => Promise<void>
  readonly resolveTask: (taskId: string) => Promise<A2aTaskRow | undefined>
  readonly saveTask: (row: A2aTaskRow) => Promise<void>
  /** Save a full task state (for persistent TaskStore). */
  readonly saveTaskState: (taskId: string, state: string) => Promise<void>
  /** Load a full task state (for persistent TaskStore). */
  readonly loadTaskState: (taskId: string) => Promise<string | undefined>
  /** List task states with optional pagination. */
  readonly listTaskStates: (pageSize: number, pageToken: string) => Promise<{ states: { taskId: string; state: string }[]; nextPageToken: string }>
  /** List context rows that are stale (for session removal before pruning). */
  readonly staleContexts: (cutoff: number) => Promise<A2aContextRow[]>
  /** Delete a context row by contextId. */
  readonly deleteContext: (contextId: string) => Promise<void>
  /** Prune stale context and task rows. Call periodically. */
  readonly prune: () => Promise<void>
  readonly close: () => void
}

export class A2aDbService extends Context.Service<A2aDbService, A2aDb>()("@opencode/A2aDb") {}

function resolveDataDir(): string {
  const envDb = process.env.OPENCODE_DB
  if (envDb && envDb !== ":memory:") return path.dirname(envDb)
  const home = process.env.HOME ?? process.env.USERPROFILE ?? "."
  const xdg = process.env.XDG_DATA_HOME
  return xdg ?? path.join(home, ".local", "share")
}

export const A2aDbLayer = Layer.sync(
  A2aDbService,
  () => {
    const dataDir = resolveDataDir()
    const dbPath = path.join(dataDir, "opencode", "a2a.db")

    const { Database } = require("bun:sqlite")
    const sqlite = new Database(dbPath, { create: true })
    sqlite.exec("PRAGMA journal_mode = WAL")
    sqlite.exec("PRAGMA synchronous = NORMAL")
    sqlite.exec("PRAGMA foreign_keys = ON")

    sqlite.exec(`
      CREATE TABLE IF NOT EXISTS a2a_context (
        context_id  TEXT PRIMARY KEY,
        session_id  TEXT NOT NULL,
        directory   TEXT NOT NULL,
        created_at  INTEGER NOT NULL,
        last_used_at INTEGER NOT NULL DEFAULT 0
      )
    `)
    sqlite.exec(`
      CREATE TABLE IF NOT EXISTS a2a_task (
        task_id     TEXT PRIMARY KEY,
        context_id  TEXT NOT NULL,
        session_id  TEXT NOT NULL,
        created_at  INTEGER NOT NULL
      )
    `)
    sqlite.exec(`
      CREATE TABLE IF NOT EXISTS a2a_task_state (
        task_id     TEXT PRIMARY KEY,
        state       TEXT NOT NULL,
        updated_at  INTEGER NOT NULL
      )
    `)
    // Add last_used_at before creating its index: existing DBs predate the column.
    const hasLastUsedAt = (
      sqlite.prepare("SELECT COUNT(*) AS n FROM pragma_table_info('a2a_context') WHERE name = 'last_used_at'").get() as { n: number }
    ).n > 0
    if (!hasLastUsedAt) sqlite.exec("ALTER TABLE a2a_context ADD COLUMN last_used_at INTEGER NOT NULL DEFAULT 0")
    // peer_url records which A2A peer endpoint a thread belongs to, learned from
    // outbound a2a_call replies (the a2a tool writes it directly). Nullable:
    // inbound-only threads have no known reply URL.
    const hasPeerUrl = (
      sqlite.prepare("SELECT COUNT(*) AS n FROM pragma_table_info('a2a_context') WHERE name = 'peer_url'").get() as { n: number }
    ).n > 0
    if (!hasPeerUrl) sqlite.exec("ALTER TABLE a2a_context ADD COLUMN peer_url TEXT")
    sqlite.exec(`CREATE INDEX IF NOT EXISTS a2a_task_context_idx ON a2a_task(context_id)`)
    sqlite.exec(`CREATE INDEX IF NOT EXISTS a2a_context_last_used_idx ON a2a_context(last_used_at)`)
    sqlite.exec(`CREATE INDEX IF NOT EXISTS a2a_task_created_idx ON a2a_task(created_at)`)

    const contextQuery = sqlite.prepare("SELECT context_id, session_id, directory, created_at, last_used_at FROM a2a_context WHERE context_id = ?")
    const contextTouch = sqlite.prepare("UPDATE a2a_context SET last_used_at = ? WHERE context_id = ?")
    const taskQuery = sqlite.prepare("SELECT task_id, context_id, session_id, created_at FROM a2a_task WHERE task_id = ?")
    const taskInsert = sqlite.prepare("INSERT OR REPLACE INTO a2a_task (task_id, context_id, session_id, created_at) VALUES (?, ?, ?, ?)")
    const taskStateInsert = sqlite.prepare("INSERT OR REPLACE INTO a2a_task_state (task_id, state, updated_at) VALUES (?, ?, ?)")
    const taskStateQuery = sqlite.prepare("SELECT state FROM a2a_task_state WHERE task_id = ?")
    const contextPrune = sqlite.prepare("DELETE FROM a2a_context WHERE last_used_at > 0 AND last_used_at < ?")
    const taskPrune = sqlite.prepare("DELETE FROM a2a_task WHERE created_at < ?")
    const taskStatePrune = sqlite.prepare("DELETE FROM a2a_task_state WHERE updated_at < ?")
    const staleContextsQuery = sqlite.prepare("SELECT context_id, session_id, directory, created_at FROM a2a_context WHERE last_used_at > 0 AND last_used_at < ?")
    const contextDelete = sqlite.prepare("DELETE FROM a2a_context WHERE context_id = ?")
    const taskStateList = sqlite.prepare("SELECT task_id, state FROM a2a_task_state WHERE task_id > ? ORDER BY task_id LIMIT ?")

    let lastPrune = 0

    const db: A2aDb = {
      resolveContext: (contextId: string) => {
        // Prune stale entries at most once per 10 minutes
        const now = Date.now()
        if (now - lastPrune > 10 * 60 * 1000) {
          lastPrune = now
          contextPrune.run(now - SESSION_TTL_MS)
          taskPrune.run(now - TASK_TTL_MS)
          taskStatePrune.run(now - TASK_TTL_MS)
        }
        const row = contextQuery.get(contextId) as { context_id: string; session_id: string; directory: string; created_at: number; last_used_at: number } | null
        if (!row) return Promise.resolve(undefined)
        // Touch last_used_at so active contexts aren't pruned
        contextTouch.run(now, contextId)
        return Promise.resolve({ contextId: row.context_id, sessionID: row.session_id, directory: row.directory, createdAt: row.created_at })
      },
      saveContext: (row: A2aContextRow) => {
        // Upsert (not INSERT OR REPLACE) so a re-save of an existing context
        // preserves created_at and the tool-written peer_url.
        sqlite
          .prepare(
            `INSERT INTO a2a_context (context_id, session_id, directory, created_at, last_used_at) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(context_id) DO UPDATE SET
               session_id = excluded.session_id,
               directory = excluded.directory,
               last_used_at = excluded.last_used_at`,
          )
          .run(row.contextId, row.sessionID, row.directory, row.createdAt, row.createdAt)
        return Promise.resolve()
      },
      resolveTask: (taskId: string) => {
        const row = taskQuery.get(taskId) as { task_id: string; context_id: string; session_id: string; created_at: number } | null
        if (!row) return Promise.resolve(undefined)
        return Promise.resolve({ taskId: row.task_id, contextId: row.context_id, sessionID: row.session_id, createdAt: row.created_at })
      },
      saveTask: (row: A2aTaskRow) => {
        taskInsert.run(row.taskId, row.contextId, row.sessionID, row.createdAt)
        return Promise.resolve()
      },
      saveTaskState: (taskId: string, state: string) => {
        taskStateInsert.run(taskId, state, Date.now())
        return Promise.resolve()
      },
      loadTaskState: (taskId: string) => {
        const row = taskStateQuery.get(taskId) as { state: string } | null
        return Promise.resolve(row?.state)
      },
      listTaskStates: (pageSize: number, pageToken: string) => {
        const rows = taskStateList.all(pageToken, pageSize) as { task_id: string; state: string }[]
        const states = rows.map((r) => ({ taskId: r.task_id, state: r.state }))
        const nextPageToken = rows.length === pageSize ? (rows[rows.length - 1]?.task_id ?? "") : ""
        return Promise.resolve({ states, nextPageToken })
      },
      staleContexts: (cutoff: number) => {
        const rows = staleContextsQuery.all(cutoff) as { context_id: string; session_id: string; directory: string; created_at: number }[]
        return Promise.resolve(rows.map((r) => ({ contextId: r.context_id, sessionID: r.session_id, directory: r.directory, createdAt: r.created_at })))
      },
      deleteContext: (contextId: string) => {
        contextDelete.run(contextId)
        return Promise.resolve()
      },
      prune: () => {
        const now = Date.now()
        contextPrune.run(now - SESSION_TTL_MS)
        taskPrune.run(now - TASK_TTL_MS)
        taskStatePrune.run(now - TASK_TTL_MS)
        return Promise.resolve()
      },
      close: () => sqlite.close(),
    }

    return A2aDbService.of(db)
  },
)
