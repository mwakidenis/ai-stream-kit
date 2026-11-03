export interface NDJSONOptions {
  /** Maximum buffered characters before flushing (optional safety valve). Default: no limit. */
  maxBufferSize?: number;
}

export interface NDJSONEvent<T = unknown> {
  /** The fully parsed JSON object from a complete newline-delimited line. */
  value: T;
}

class PendingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PendingNDJSONError';
  }
}

/**
 * NDJSONTransform splits a raw text stream on newlines and emits each complete
 * line as a parsed JavaScript object. Handles JSON objects broken across
 * multiple network chunks by buffering partial lines until a delimiter arrives.
 */
export class NDJSONTransform<T = unknown> {
  private buffer = '';
  private size = 0;
  private controller?: TransformStreamDefaultController<T>;
  private ended = false;

  private readonly options: NDJSONOptions;

  constructor(options: NDJSONOptions = {}) {
    this.options = options;
  }

  /** The WHATWG TransformStream consumed by producers and read by consumers. */
  get stream(): TransformStream<string, T> {
    return new TransformStream<string, T>({
      transform: (chunk, controller) => {
        this.controller = controller;
        this.buffer += chunk;
        this.size += chunk.length;

        if (this.options.maxBufferSize && this.size > this.options.maxBufferSize) {
          this.buffer = '';
          this.size = 0;
          this.controller = undefined;
          throw new PendingError(
            `NDJSONTransform buffer exceeded maxBufferSize of ${this.options.maxBufferSize}`,
          );
        }

        let index: number;
        while ((index = this.buffer.indexOf('\n')) !== -1) {
          const line = this.buffer.slice(0, index).replace(/\r$/, '');
          this.buffer = this.buffer.slice(index + 1);
          this.size = this.buffer.length;
          const trimmed = line.trim();
          if (trimmed.length === 0) continue;
          controller.enqueue(JSON.parse(trimmed) as T);
        }
      },
      flush: (controller) => {
        this.controller = controller;
        this.ended = true;
        const trimmed = this.buffer.trim();
        this.buffer = '';
        this.size = 0;
        if (trimmed.length > 0) {
          controller.enqueue(JSON.parse(trimmed) as T);
        }
      },
    });
  }
}
