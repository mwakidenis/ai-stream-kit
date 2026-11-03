import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { SSEParserTransform } from '../src/index.ts';

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

describe('SSEParserTransform', async () => {
  test('parses a basic data event', async () => {
    const t = new SSEParserTransform();
    const result = await collect(
      textStream(['data: {"hello":"world"}\n\n']).pipeThrough(t.stream),
    );
    assert.deepStrictEqual(result, [{ event: 'message', data: { hello: 'world' } }]);
  });

  test('parses event, id, and data fields', async () => {
    const t = new SSEParserTransform();
    const result = await collect(
      textStream(['event: update\nid: 42\ndata: {"ok":true}\n\n']).pipeThrough(t.stream),
    );
    assert.deepStrictEqual(result, [{ event: 'update', id: '42', data: { ok: true } }]);
  });

  test('joins multi-line data and parses as JSON', async () => {
    const t = new SSEParserTransform();
    const result = await collect(
      textStream(['data: {"line":1}\ndata: {"line":2}\n\n']).pipeThrough(t.stream),
    );
    assert.deepStrictEqual(result, [{ event: 'message', data: '{"line":1}\n{"line":2}' }]);
  });

  test('ignores comment lines and keeps raw data when not JSON', async () => {
    const t = new SSEParserTransform();
    const result = await collect(
      textStream([': this is a comment\ndata: hello world\n\n']).pipeThrough(t.stream),
    );
    assert.deepStrictEqual(result, [{ event: 'message', data: 'hello world' }]);
  });

  test('handles events split across chunks', async () => {
    const t = new SSEParserTransform();
    const result = await collect(
      textStream(['data: {"n":', '1}\n\n']).pipeThrough(t.stream),
    );
    assert.deepStrictEqual(result, [{ event: 'message', data: { n: 1 } }]);
  });

  test('flushes a final unterminated event on stream end', async () => {
    const t = new SSEParserTransform();
    const result = await collect(textStream(['data: final\n']).pipeThrough(t.stream));
    assert.deepStrictEqual(result, [{ event: 'message', data: 'final' }]);
  });
});
