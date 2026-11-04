export declare class NDJSONTransform {
  constructor(options?: { maxBufferSize?: number });
  get stream(): TransformStream<string, unknown>;
}
export declare class TokenBufferTransform {
  constructor(options?: { maxSize?: number });
  get stream(): TransformStream<string, string>;
}
export declare class SSEParserTransform {
  constructor(options?: { maxBufferSize?: number });
  get stream(): TransformStream<string, { event: string; id?: string; data: unknown }>;
}
