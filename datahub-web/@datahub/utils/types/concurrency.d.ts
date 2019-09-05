import { Task, TaskInstance } from 'ember-concurrency';

/**
 * Helper types to reduce the amount of code to define a Task
 */
type ETask<T, T1 = undefined, T2 = undefined, T3 = undefined> = T1 extends undefined
  ? Task<TaskInstance<T>, () => TaskInstance<T>>
  : T2 extends undefined
  ? Task<TaskInstance<T>, (a: T1) => TaskInstance<T>>
  : T3 extends undefined
  ? Task<TaskInstance<T>, (a: T1, b: T2) => TaskInstance<T>>
  : Task<TaskInstance<T>, (a: T1, b: T2, c: T3) => TaskInstance<T>>;

/**
 * Same as ETask but instead of returning a TaskInstance, returns a Promise which is a common practice
 */
type ETaskPromise<T = void, T1 = undefined, T2 = undefined, T3 = undefined> = T1 extends undefined
  ? Task<Promise<T>, () => Promise<T>>
  : T2 extends undefined
  ? Task<Promise<T>, (a: T1) => Promise<T>>
  : T3 extends undefined
  ? Task<Promise<T>, (a: T1, b: T2) => Promise<T>>
  : Task<Promise<T>, (a: T1, b: T2, c: T3) => Promise<T>>;
