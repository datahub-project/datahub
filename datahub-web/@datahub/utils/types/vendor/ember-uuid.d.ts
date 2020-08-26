declare module 'ember-uuid' {
  /**
   * Optional uuid state to apply
   * @interface IUuidState
   */
  interface IUuidStateV1 {
    // Node id as Array of 6 bytes
    node?: [number, number, number, number, number, number];
    //  RFC clock sequence. Default: An internally maintained clockseq is used.
    clockseq?: number;
    // Time in milliseconds since unix Epoch. Default: The current time is used.
    msecs?: number;
    // additional time, in 100-nanosecond units. Ignored if msecs is unspecified. Default: internal uuid counter is used
    nsecs?: number;
  }

  type Buf = Array<number> | Buffer;

  interface IUuidStateV4 {
    // Array of 16 numbers (0-255) to use in place of randomly generated values
    random: [
      number,
      number,
      number,
      number,
      number,
      number,
      number,
      number,
      number,
      number,
      number,
      number,
      number,
      number,
      number,
      number
    ];
    // Random # generator function that returns an Array[16] of byte values (0-255)
    rng?: () => IUuidStateV4['random'];
  }

  export const v1: (options?: string | IUuidStateV1, buf?: Buf, offset?: number) => string | Buf;
  export const v4: (options?: string | IUuidStateV4, buf?: Buf, offset?: number) => string | Buf;
  export const parse: (uuid: string, buf: Buf, offset: number) => string;
  export const unparse: (buf: Buf, offset: number) => string;
}
