declare module 'decompress' {
  function decompress(
    input: string,
    output?: string,
    options?: {
      filter?: any;
      map?: any;
      strip?: number;
      plugins?: string[];
    }
  ): Promise<
    Array<{
      path: string;
      type: string;
      data: Buffer;
      mode: number;
      mtime: string;
    }>
  >;
  export = decompress;
}
