const CR = '\r';
const LF = '\n';
const BLANK = '\n\n';

function parseData(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

class NDJSONTransform {
  constructor(options = {}) {
    this.buffer = '';
    this.options = options;
    this.controller = null;
    this.ended = false;
  }

  get stream() {
    return new TransformStream({
      transform: (chunk, controller) => {
        this.controller = controller;
        this.buffer += chunk;
        let idx;
        while ((idx = this.buffer.indexOf(LF)) !== -1) {
          const line = this.buffer.slice(0, idx).replace(CR + '$', '');
          this.buffer = this.buffer.slice(idx + 1);
          const trimmed = line.trim();
          if (trimmed.length > 0) {
            controller.enqueue(JSON.parse(trimmed));
          }
        }
      },
      flush: (controller) => {
        this.controller = controller;
        this.ended = true;
        const trimmed = this.buffer.trim();
        this.buffer = '';
        if (trimmed.length > 0) {
          controller.enqueue(JSON.parse(trimmed));
        }
      },
    });
  }
}

class TokenBufferTransform {
  constructor(options = {}) {
    this.buffer = '';
    this.size = 0;
    this.options = options;
    this.controller = null;
    this.ended = false;
  }

  get stream() {
    const maxSize = this.options.maxSize || 512;
    return new TransformStream({
      transform: (chunk, controller) => {
        this.controller = controller;
        this.buffer += chunk;
        this.size += chunk.length;
        if (this.size >= maxSize) {
          controller.enqueue(this.buffer);
          this.buffer = '';
          this.size = 0;
        }
      },
      flush: (controller) => {
        this.controller = controller;
        this.ended = true;
        if (this.buffer.length > 0) {
          controller.enqueue(this.buffer);
          this.buffer = '';
          this.size = 0;
        }
      },
    });
  }
}

class SSEParserTransform {
  constructor(options = {}) {
    this.buffer = '';
    this.options = options;
    this.controller = null;
    this.ended = false;
  }

  get stream() {
    return new TransformStream({
      transform: (chunk, controller) => {
        this.controller = controller;
        this.buffer += chunk;
        let idx;
        while ((idx = this.buffer.indexOf(BLANK)) !== -1) {
          const raw = this.buffer.slice(0, idx);
          this.buffer = this.buffer.slice(idx + 2);
          const event = this._parse(raw);
          if (event) controller.enqueue(event);
        }
      },
      flush: (controller) => {
        this.controller = controller;
        this.ended = true;
        const trimmed = this.buffer.trim();
        this.buffer = '';
        if (trimmed.length > 0) {
          const event = this._parse(trimmed);
          if (event) controller.enqueue(event);
        }
      },
    });
  }

  _parse(raw) {
    const lines = raw.split(/\r?\n/);
    let event = 'message';
    let id;
    const dataParts = [];

    for (const line of lines) {
      if (line.length === 0) continue;
      if (line.startsWith(':')) continue;

      const colon = line.indexOf(':');
      if (colon === -1) continue;

      const field = line.slice(0, colon);
      let value = line.slice(colon + 1);
      if (value.startsWith(' ')) value = value.slice(1);

      if (field === 'event') event = value;
      else if (field === 'id') id = value;
      else if (field === 'data') dataParts.push(value);
    }

    if (dataParts.length === 0 && id === undefined && event === 'message') {
      return null;
    }

    const out = { event };
    if (id !== undefined) out.id = id;
    out.data = parseData(dataParts.join(LF));
    return out;
  }
}

export { NDJSONTransform, TokenBufferTransform, SSEParserTransform };
