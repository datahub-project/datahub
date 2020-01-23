/**
 * Exports a noop that can be used in place of Ember.K which is currently deprecated.
 */
// Note, the very nature of noop is to be of any typing because it's noop.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const noop: (...args: Array<any>) => any = (): any => {};
