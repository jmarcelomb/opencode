import os from "node:os"
import path from "node:path"
import { writeFileSync } from "node:fs"

const MIME_EXTENSIONS: Record<string, string> = {
  "image/png": ".png",
  "image/jpeg": ".jpg",
  "image/gif": ".gif",
  "image/webp": ".webp",
  "image/avif": ".avif",
  "image/svg+xml": ".svg",
}

// Clipboard images arrive as inline data URIs with no filesystem path.
// Write the decoded bytes to a temp file so the user can unwrap the pill to a
// path that text models can pass to a vision proxy tool.
export function writeClipboardImageToTemp(uri: string): string {
  const match = uri.match(/^data:(image\/[a-z.+]+);base64,(.*)$/i)
  if (!match) return ""
  const [, mime, base64] = match
  const ext = MIME_EXTENSIONS[mime] ?? `.${mime.split("/")[1].split("+")[0].toLowerCase()}`
  const filepath = path.join(os.tmpdir(), `clipboard-${Date.now()}-${Math.random().toString(36).slice(2, 8)}${ext}`)
  try {
    writeFileSync(filepath, Buffer.from(base64, "base64"))
  } catch {
    return ""
  }
  return filepath
}
