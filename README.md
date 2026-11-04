# @mwakidenis/ai-stream-kit

Lightweight NDJSON & SSE / Token Stream Transformer built on standard WHATWG Web Streams (`TransformStream`). Zero dependencies. No build step. Instant install, instant deploy on Vercel, Netlify, Cloudflare Pages, and any Node/Bun/Deno runtime.

## Install

```bash
npm install @mwakidenis/ai-stream-kit@2.0.0
```

## API

All transformers expose a `.stream` getter that returns a `TransformStream`.

### NDJSONTransform

Splits a raw text stream on newlines and emits each complete line as a parsed JavaScript object. Handles JSON objects broken across chunks.

```ts
import { NDJSONTransform } from '@mwakidenis/ai-stream-kit';

const ndjson = new NDJSONTransform();
const stream = responseBody.pipeThrough(ndjson.stream);

for await (const obj of stream) {
  console.log(obj);
}
```

### TokenBufferTransform

Batches rapid chunks by size threshold only — **no artificial delay**. Flushes immediately when `maxSize` is reached, and flushes any remainder on stream end.

```ts
import { TokenBufferTransform } from '@mwakidenis/ai-stream-kit';

const buffer = new TokenBufferTransform({ maxSize: 512 });
const stream = tokenStream.pipeThrough(buffer.stream);

for await (const chunk of stream) {
  updateUI(chunk);
}
```

### SSEParserTransform

Parses raw Server-Sent Events (`data: {...}\n\n`) into `{ event, id?, data }` objects.

```ts
import { SSEParserTransform } from '@mwakidenis/ai-stream-kit';

const sse = new SSEParserTransform();
const stream = eventSource.pipeThrough(sse.stream);

for await (const event of stream) {
  console.log(event);
}
```

## Options

| Transformer | Option | Default |
|-------------|--------|---------|
| NDJSONTransform | `maxBufferSize` | `undefined` (no limit) |
| TokenBufferTransform | `maxSize` | `512` |
| SSEParserTransform | `maxBufferSize` | `undefined` (no limit) |

## Compatibility

Node.js 18+, Bun, Deno, Cloudflare Workers/Pages, Vercel, Netlify. Uses only WHATWG Web Streams — no polyfills, no Node-only APIs, no build step.

## Test

```bash
npm test
```

## License

MIT
