declare module 'rsvp' {
  export type OnFulfilled<T, TResult1> = ((value: T) => TResult1 | PromiseLike<TResult1>) | undefined | null;
  export type OnRejected<T, TResult2> = ((reason: any) => TResult2 | PromiseLike<TResult2>) | undefined | null;
}
