import assert from "node:assert/strict";
import test from "node:test";

test("uses the pinned Node 24 toolchain", () => {
  const majorVersion = Number.parseInt(process.versions.node.split(".", 1)[0], 10);

  assert.equal(majorVersion, 24);
});
