import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { TokenBufferTransform } from '../src/index.mjs';

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

describe('TokenBufferTransform', async () => {
  test('emits when size threshold is reached', async () => {
    const t = new TokenBufferTransform({ maxSize: 6 });
    const res = await collect(chunksToStream(['ab', 'cd', 'ef', 'gh']).pipeThrough(t.stream));
    assert.deepStrictEqual(res, ['abcdef', 'gh']);
  });

  test('flushes remaining buffer on stream end', async () => {
    const t = new TokenBufferTransform({ maxSize: 1000 });
    const res = await collect(chunksToStream(['hello']).pipeThrough(t.stream));
    assert.deepStrictEqual(res, ['hello']);
  });

  test('emits nothing for empty input', async () => {
    const t = new TokenBufferTransform({ maxSize: 10 });
    const res = await collect(chunksToStream([]).pipeThrough(t.stream));
    assert.deepStrictEqual(res, []);
  });
});
