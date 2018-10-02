import { TaskInstance } from 'ember-concurrency';

/**
 * A task can be used instead of a promise in some cases, but a task
 * has the advantage of being cancellable. See ember-concurrency.
 */
export type PromiseOrTask<T> = PromiseLike<T> | TaskInstance<T> | undefined;

/**
 * Will check if the type is a promise or a task. The difference is that
 * a task is cancellable where as a promise not (for now).
 * @param obj the object to check
 */
export function isTask<T>(obj: PromiseOrTask<T>): obj is TaskInstance<T> {
  return typeof obj !== 'undefined' && (<TaskInstance<T>>obj).cancel !== undefined;
}
