export interface SSEOptions {
  /** Maximum buffered characters before flushing (optional safety valve). Default: no limit. */
  maxBufferSize?: number;
}

export interface SSEEvent {
  /** Event type, defaults to "message". */
  event: string;
  /** Optional event id. */
  id?: string;
  /** Parsed data payload. Parsed as JSON when possible, otherwise the raw string. */
  data: unknown;
}

function parseData(raw: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

/**
 * SSEParserTransform parses raw Server-Sent Events text into structured
 * { event, id, data } objects. Events are terminated by a blank line ("\n\n").
 * Multi-line `data:` fields are joined with "\n" and parsed as JSON when possible.
 */
export class SSEParserTransform {
  private buffer = '';
  private size = 0;
  private controller?: TransformStreamDefaultController<SSEEvent>;
  private ended = false;

  private readonly options: SSEOptions;

  constructor(options: SSEOptions = {}) {
    this.options = options;
  }

  get stream(): TransformStream<string, SSEEvent> {
    return new TransformStream<string, SSEEvent>({
      transform: (chunk, controller) => {
        this.controller = controller;
        this.buffer += chunk;
        this.size += chunk.length;

        if (this.options.maxBufferSize && this.size > this.options.maxBufferSize) {
          this.buffer = '';
          this.size = 0;
          this.controller = undefined;
          throw new Error(
            `SSEParserTransform buffer exceeded maxBufferSize of ${this.options.maxBufferSize}`,
          );
        }

        let index: number;
        while ((index = this.indexOfBlankLine(this.buffer)) !== -1) {
          const rawEvent = this.buffer.slice(0, index);
          this.buffer = this.buffer.slice(index + 2);
          this.size = this.buffer.length;
          const event = this.parseEvent(rawEvent);
          if (event) controller.enqueue(event);
        }
      },
      flush: (controller) => {
        this.controller = controller;
        this.ended = true;
        const trimmed = this.buffer.trim();
        this.buffer = '';
        this.size = 0;
        if (trimmed.length > 0) {
          const event = this.parseEvent(trimmed);
          if (event) controller.enqueue(event);
        }
      },
    });
  }

  /** Locate "\n\n" (with optional "\r\n" line endings) terminator. */
  private indexOfBlankLine(text: string): number {
    // Normalize \r\n to \n for reliable blank-line detection.
    const normalized = text.replace(/\r\n/g, '\n');
    return normalized.indexOf('\n\n');
  }

  private parseEvent(raw: string): SSEEvent | null {
    const lines = raw.split(/\r?\n/);
    let event = 'message';
    let id: string | undefined;
    const dataParts: string[] = [];

    for (const line of lines) {
      if (line.length === 0) continue;
      // Comment lines starting with ':' are ignored.
      if (line.startsWith(':')) continue;

      const colon = line.indexOf(':');
      if (colon === -1) continue;

      const field = line.slice(0, colon);
      let value = line.slice(colon + 1);
      if (value.startsWith(' ')) value = value.slice(1);

      if (field === 'event') {
        event = value;
      } else if (field === 'id') {
        id = value;
      } else if (field === 'data') {
        dataParts.push(value);
      }
      // Unknown fields are silently ignored.
    }

    if (dataParts.length === 0 && id === undefined && event === 'message') {
      return null;
    }

    return {
      event,
      ...(id ? { id } : {}),
      data: parseData(dataParts.join('\n')),
    };
  }
}
