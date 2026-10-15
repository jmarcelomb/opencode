import { describe, expect, test } from "bun:test"
import { existsSync, readFileSync } from "node:fs"
import { writeClipboardImageToTemp } from "../../src/component/prompt/image-temp"

describe("writeClipboardImageToTemp", () => {
  test("writes clipboard image bytes to a temp file and returns the path", () => {
    const uri = "data:image/png;base64,AQID"
    const filepath = writeClipboardImageToTemp(uri)
    expect(filepath).toBeTruthy()
    expect(filepath).toContain("clipboard-")
    expect(filepath).toEndWith(".png")
    expect(existsSync(filepath)).toBe(true)
    expect(readFileSync(filepath)).toEqual(Buffer.from([1, 2, 3]))
  })

  test("maps jpeg mime to .jpg extension", () => {
    const filepath = writeClipboardImageToTemp("data:image/jpeg;base64,AQID")
    expect(filepath).toEndWith(".jpg")
  })

  test("maps webp mime to .webp extension", () => {
    const filepath = writeClipboardImageToTemp("data:image/webp;base64,AQID")
    expect(filepath).toEndWith(".webp")
  })

  test("derives extension from mime subtype for unmapped types", () => {
    const filepath = writeClipboardImageToTemp("data:image/bmp;base64,AQID")
    expect(filepath).toEndWith(".bmp")
  })

  test("returns empty string for non-data-URI input", () => {
    expect(writeClipboardImageToTemp("https://example.com/image.png")).toBe("")
  })

  test("returns empty string for non-image data URIs", () => {
    expect(writeClipboardImageToTemp("data:application/pdf;base64,AQID")).toBe("")
  })

  test("handles svg+xml mime type", () => {
    const filepath = writeClipboardImageToTemp("data:image/svg+xml;base64,PHN2ZyAvPg==")
    expect(filepath).toEndWith(".svg")
    expect(readFileSync(filepath).toString()).toBe("<svg />")
  })
})
