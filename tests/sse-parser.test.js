import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { SSEParserTransform } from '../src/index.mjs';

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

describe('SSEParserTransform', async () => {
  test('parses a basic data event', async () => {
    const t = new SSEParserTransform();
    const res = await collect(chunksToStream(['data: {"hello":"world"}\n\n']).pipeThrough(t.stream));
    assert.deepStrictEqual(res, [{ event: 'message', data: { hello: 'world' } }]);
  });

  test('parses event, id, and data fields', async () => {
    const t = new SSEParserTransform();
    const res = await collect(chunksToStream(['event: update\nid: 42\ndata: {"ok":true}\n\n']).pipeThrough(t.stream));
    assert.deepStrictEqual(res, [{ event: 'update', id: '42', data: { ok: true } }]);
  });

  test('ignores comment lines and keeps raw data when not JSON', async () => {
    const t = new SSEParserTransform();
    const res = await collect(chunksToStream([': comment\ndata: hello world\n\n']).pipeThrough(t.stream));
    assert.deepStrictEqual(res, [{ event: 'message', data: 'hello world' }]);
  });

  test('handles events split across chunks', async () => {
    const t = new SSEParserTransform();
    const res = await collect(chunksToStream(['data: {"n":', '1}\n\n']).pipeThrough(t.stream));
    assert.deepStrictEqual(res, [{ event: 'message', data: { n: 1 } }]);
  });

  test('flushes a final unterminated event on stream end', async () => {
    const t = new SSEParserTransform();
    const res = await collect(chunksToStream(['data: final\n']).pipeThrough(t.stream));
    assert.deepStrictEqual(res, [{ event: 'message', data: 'final' }]);
  });
});
