import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { TokenBufferTransform } from '../src/index.ts';

function textStream(chunks: string[]): ReadableStream<string> {
  return new ReadableStream<string>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

async function collect<T>(stream: ReadableStream<T>): Promise<T[]> {
  const reader = stream.getReader();
  const out: T[] = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    out.push(value);
  }
  return out;
}

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('TokenBufferTransform', async () => {
  test('emits when size threshold is reached', async () => {
    const t = new TokenBufferTransform({ maxSize: 6, maxWait: 1000 });
    const result = await collect(textStream(['ab', 'cd', 'ef', 'gh']).pipeThrough(t.stream));
    // 'ab'+'cd'=4, +'ef'=6 reaches threshold -> emit 'abcdef'; then 'gh' flushed at end
    assert.deepStrictEqual(result, ['abcdef', 'gh']);
  });

  test('emits after the time window elapses', async () => {
    const t = new TokenBufferTransform({ maxSize: 10000, maxWait: 20 });
    const stream = textStream(['a', 'b', 'c']).pipeThrough(t.stream);
    const reader = stream.getReader();
    const first = await reader.read();
    assert.strictEqual(first.done, false);
    assert.strictEqual(first.value, 'abc');
  });

  test('flushes remaining buffer on stream end', async () => {
    const t = new TokenBufferTransform({ maxSize: 1000, maxWait: 1000 });
    const result = await collect(textStream(['hello']).pipeThrough(t.stream));
    assert.deepStrictEqual(result, ['hello']);
  });

  test('emits nothing for empty input', async () => {
    const t = new TokenBufferTransform({ maxSize: 10, maxWait: 10 });
    const result = await collect(textStream([]).pipeThrough(t.stream));
    assert.deepStrictEqual(result, []);
  });
});
