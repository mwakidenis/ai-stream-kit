import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { NDJSONTransform } from '../src/index.mjs';

function chunksToStream(chunks) {
  return new ReadableStream({
    start(controller) {
      for (const c of chunks) controller.enqueue(c);
      controller.close();
    },
  });
}

async function collect(stream) {
  const reader = stream.getReader();
  const out = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    out.push(value);
  }
  return out;
}

describe('NDJSONTransform', async () => {
  test('parses complete newline-delimited JSON lines', async () => {
    const t = new NDJSONTransform();
    const res = await collect(chunksToStream(['{"n":1}\n{"n":2}\n']).pipeThrough(t.stream));
    assert.deepStrictEqual(res, [{ n: 1 }, { n: 2 }]);
  });

  test('handles a JSON object split across chunks', async () => {
    const t = new NDJSONTransform();
    const res = await collect(chunksToStream(['{"msg":"hel', 'lo world"}\n']).pipeThrough(t.stream));
    assert.deepStrictEqual(res, [{ msg: 'hello world' }]);
  });

  test('flushes a trailing line without a newline', async () => {
    const t = new NDJSONTransform();
    const res = await collect(chunksToStream(['{"n":7}']).pipeThrough(t.stream));
    assert.deepStrictEqual(res, [{ n: 7 }]);
  });

  test('skips empty lines and handles CRLF', async () => {
    const t = new NDJSONTransform();
    const res = await collect(chunksToStream(['\n{"n":1}\r\n\r\n{"n":2}\n']).pipeThrough(t.stream));
    assert.deepStrictEqual(res, [{ n: 1 }, { n: 2 }]);
  });
});
