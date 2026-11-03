export interface TokenBufferOptions {
  /** Emit once the accumulated buffer reaches this many characters. Default: 512. */
  maxSize?: number;
  /** Emit after this many milliseconds of silence since the last chunk. Disabled by default (no artificial delay). Set a value > 0 to enable time-window flushing. */
  maxWait?: number;
}

/**
 * TokenBufferTransform batches rapid incoming token/text chunks into slightly
 * larger chunks to prevent UI rendering bottlenecks. Emits when the size
 * threshold is reached, after an optional time window of silence, or on stream
 * end. No artificial delay is introduced unless maxWait is explicitly set.
 */
export class TokenBufferTransform {
  private buffer = '';
  private size = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private controller?: TransformStreamDefaultController<string>;
  private ended = false;

  private readonly options: TokenBufferOptions;

  constructor(options: TokenBufferOptions = {}) {
    this.options = options;
  }

  get stream(): TransformStream<string, string> {
    const maxSize = this.options.maxSize ?? 512;
    const maxWait = this.options.maxWait;

    const scheduleFlush = () => {
      if (this.timer) clearTimeout(this.timer);
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.doFlush(this.controller!);
      }, maxWait!);
    };

    return new TransformStream<string, string>({
      transform: (chunk, controller) => {
        this.controller = controller;
        this.buffer += chunk;
        this.size += chunk.length;

        if (this.size >= maxSize) {
          this.doFlush(controller);
          if (this.timer) {
            clearTimeout(this.timer);
            this.timer = undefined;
          }
          return;
        }

        if (maxWait && !this.timer) scheduleFlush();
      },
      flush: (controller) => {
        this.controller = controller;
        this.ended = true;
        if (this.timer) {
          clearTimeout(this.timer);
          this.timer = undefined;
        }
        this.doFlush(controller);
      },
    });
  }

  private doFlush(controller: TransformStreamDefaultController<string>) {
    if (this.buffer.length === 0) return;
    controller.enqueue(this.buffer);
    this.buffer = '';
    this.size = 0;
  }
}
