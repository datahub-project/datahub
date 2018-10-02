import { PromiseOrTask } from 'wherehows-web/typings/generic';
import { TaskInstance } from 'ember-concurrency';

/**
 * Negates a boolean function
 * @param {(arg: T) => boolean} fn the boolean function to negate
 * @return {(arg: T) => boolean} curried function that will receive the arg to the boolean function
 */
const not = <T>(fn: (arg: T) => boolean) => (arg: T) => !fn(arg);

/**
 * Identity function, immediately returns its argument
 * @template T
 * @param {T} x
 * @return {T}
 */
const identity = <T>(x: T): T => x;

/**
 * Exports a noop that can be used in place of Ember.K which is currently deprecated.
 */
const noop: (...args: Array<any>) => any = () => {};

/**
 * Will check if the type is a promise or a task. The difference is that
 * a task is cancellable where as a promise not (for now).
 * @param obj the object to check
 */
function isTask<T>(obj: PromiseOrTask<T>): obj is TaskInstance<T> {
  return typeof obj !== 'undefined' && (<TaskInstance<T>>obj).cancel !== undefined;
}

export { not, identity, noop, isTask };
