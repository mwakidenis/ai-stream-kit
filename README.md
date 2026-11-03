# @mwakidenis/ai-stream-kit

Lightweight NDJSON & SSE / Token Stream Transformer built on standard WHATWG Web Streams (`TransformStream`). Zero dependencies. Runs natively on Node.js, Bun, Deno, and Cloudflare Workers without modification.

Designed for modern AI integrations, edge runtimes, and real-time APIs where you consume streaming endpoints (OpenAI, Claude, custom webhooks) and need to reassemble fragmented chunks into structured data.

## Install

```bash
npm install @mwakidenis/ai-stream-kit
```

## API

All transformers expose a `.stream` getter that returns a `TransformStream`. Pipe a readable text stream through it to consume structured output downstream.

### NDJSONTransform

Splits a raw text stream on newlines and emits each complete line as a parsed JavaScript object. Handles JSON objects broken across multiple network chunks by buffering partial lines until a delimiter arrives.

```ts
import { NDJSONTransform } from '@mwakidenis/ai-stream-kit';

const ndjson = new NDJSONTransform<{ id: number; text: string }>();
const stream = fetch('https://api.example.com/stream')
  .then((res) => res.body as ReadableStream<string>)
  .then((body) => body.pipeThrough(ndjson.stream));

for await (const obj of stream) {
  console.log(obj); // { id: 1, text: 'hello' }
}
```

**Options**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxBufferSize` | `number` | `undefined` | Max buffered characters before throwing (safety valve). No limit by default. |

### TokenBufferTransform

Batches rapid incoming token/text chunks into slightly larger chunks or time windows to prevent UI rendering bottlenecks on the frontend. Emits when either the size threshold or the time window elapses, whichever comes first.

```ts
import { TokenBufferTransform } from '@mwakidenis/ai-stream-kit';

const buffer = new TokenBufferTransform({ maxSize: 512, maxWait: 50 });
const stream = llmTokenStream.pipeThrough(buffer.stream);

for await (const chunk of stream) {
  render(chunk); // fewer, larger renders
}
```

**Options**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxSize` | `number` | `512` | Emit once the accumulated buffer reaches this many characters. |
| `maxWait` | `number` | `undefined` (disabled) | Emit after this many ms of silence since the last chunk. **No artificial delay by default** — set a value > 0 to enable time-window flushing. |

### SSEParserTransform

Parses raw Server-Sent Events (`data: {...}\n\n`) into structured `{ event, id?, data }` objects. Multi-line `data:` fields are joined with `\n` and parsed as JSON when possible; otherwise the raw string is returned.

```ts
import { SSEParserTransform } from '@mwakidenis/ai-stream-kit';

const sse = new SSEParserTransform();
const stream = fetch('https://api.example.com/events')
  .then((res) => res.body as ReadableStream<string>)
  .then((body) => body.pipeThrough(sse.stream));

for await (const event of stream) {
  console.log(event);
  // { event: 'message', data: { ok: true } }
  // { event: 'update', id: '42', data: { progress: 0.5 } }
}
```

**Options**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxBufferSize` | `number` | `undefined` | Max buffered characters before throwing (safety valve). No limit by default. |

## Usage Examples

### Consuming an LLM token stream (NDJSON)

```ts
import { NDJSONTransform } from '@mwakidenis/ai-stream-kit';

const res = await fetch('https://api.openai.com/v1/chat/completions', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${key}` },
  body: JSON.stringify({ model: 'gpt-4o', stream: true, messages: [...] }),
});

const ndjson = new NDJSONTransform<{ choices: [{ delta: { content?: string } }] }>();
const stream = (res.body as ReadableStream<string>).pipeThrough(ndjson.stream);

for await (const chunk of stream) {
  const content = chunk.choices?.[0]?.delta?.content;
  if (content) process.stdout.write(content);
}
```

### Throttling fast token output

```ts
import { TokenBufferTransform } from '@mwakidenis/ai-stream-kit';

const buffer = new TokenBufferTransform({ maxSize: 256, maxWait: 30 });
const stream = tokenStream.pipeThrough(buffer.stream);

for await (const chunk of stream) {
  updateUI(chunk); // renders ~30ms batches instead of every token
}
```

### Parsing Server-Sent Events

```ts
import { SSEParserTransform } from '@mwakidenis/ai-stream-kit';

const sse = new SSEParserTransform();
const stream = eventSourceStream.pipeThrough(sse.stream);

for await (const event of stream) {
  if (event.event === 'update') {
    console.log(`id=${event.id}`, event.data);
  }
}
```

## Build Output

Dual builds via `tsup`:

- `dist/index.mjs` — ESM
- `dist/index.cjs` — CommonJS
- `dist/index.d.ts` / `dist/index.d.cts` — Type declarations

```bash
npm run build
```

## Testing

Tests use Node's built-in test runner (`node --test`) with `node:assert`.

```bash
npm test
```

Typecheck:

```bash
npm run lint
```

## Compatibility

- Node.js 18+
- Bun
- Deno
- Cloudflare Workers

Built on standard WHATWG Web Streams, so no polyfills or runtime adapters are required.

### Cloudflare Workers

This package ships an ESM build (`dist/index.mjs`) and a `browser` field pointing to it, so Wrangler's bundler resolves it correctly in Workers. Since it uses only WHATWG streams (no `fs`, `http`, or Node-only APIs), it deploys without polyfills:

```ts
// wrangler.toml
# no special flags needed; the package is ESM-first

import { NDJSONTransform } from '@mwakidenis/ai-stream-kit';

export default {
  async fetch(request: Request): Promise<Response> {
    const ndjson = new NDJSONTransform<{ message: string }>();
    const stream = request.body?.pipeThrough(new TextDecoderStream())
      .pipeThrough(ndjson.stream) as ReadableStream<{ message: string }>;

    return new Response(stream, { headers: { 'content-type': 'application/x-ndjson' } });
  },
};
```

### Cloudflare Pages Functions

Install the dependency in your Pages project, then import it directly in a Pages Function. The `browser` field resolves to the ESM build, so no bundler configuration is needed:

```bash
# in your Pages project
npm install @mwakidenis/ai-stream-kit
```

```ts
// functions/stream.ts
import { SSEParserTransform, NDJSONTransform } from '@mwakidenis/ai-stream-kit';

export const onRequest = async (context: {
  request: Request;
  env: Record<string, unknown>;
  waitUntil: (promise: Promise<unknown>) => void;
}): Promise<Response> => {
  const sse = new SSEParserTransform();
  const stream = context.request.body
    ?.pipeThrough(new TextDecoderStream())
    .pipeThrough(sse.stream) as ReadableStream<{ event: string; data: unknown }>;

  return new Response(stream, {
    headers: { 'content-type': 'application/x-ndjson' },
  });
};
```

Because the package uses only WHATWG Web Streams, it runs in Pages Functions without polyfills or Node.js shims.

## License

MIT

