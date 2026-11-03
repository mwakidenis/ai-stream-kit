import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { NDJSONTransform } from '../src/index.ts';

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

describe('NDJSONTransform', async () => {
  test('parses complete newline-delimited JSON lines', async () => {
    const t = new NDJSONTransform<{ n: number }>();
    const result = await collect(textStream(['{"n":1}\n{"n":2}\n']).pipeThrough(t.stream));
    assert.deepStrictEqual(result, [{ n: 1 }, { n: 2 }]);
  });

  test('handles a JSON object split across chunks', async () => {
    const t = new NDJSONTransform<{ msg: string }>();
    const result = await collect(
      textStream(['{"msg":"hel', 'lo world"}\n']).pipeThrough(t.stream),
    );
    assert.deepStrictEqual(result, [{ msg: 'hello world' }]);
  });

  test('flushes a trailing line without a newline', async () => {
    const t = new NDJSONTransform<{ n: number }>();
    const result = await collect(textStream(['{"n":7}']).pipeThrough(t.stream));
    assert.deepStrictEqual(result, [{ n: 7 }]);
  });

  test('skips empty lines and handles CRLF', async () => {
    const t = new NDJSONTransform<{ n: number }>();
    const result = await collect(
      textStream(['\n{"n":1}\r\n\r\n{"n":2}\n']).pipeThrough(t.stream),
    );
    assert.deepStrictEqual(result, [{ n: 1 }, { n: 2 }]);
  });

  test('emits objects in order across many chunks', async () => {
    const t = new NDJSONTransform<{ i: number }>();
    const chunks: string[] = [];
    for (let i = 0; i < 20; i++) chunks.push(`{"i":${i}}\n`);
    const result = await collect(textStream(chunks).pipeThrough(t.stream));
    assert.strictEqual(result.length, 20);
    assert.deepStrictEqual(result[19], { i: 19 });
  });
});
