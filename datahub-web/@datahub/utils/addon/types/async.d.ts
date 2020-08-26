/**
 * Extract the type parameter a Promise object resolves with
 */
export type UnWrapPromise<T> = T extends PromiseLike<infer U> ? U : T;
