import { TaskInstance } from 'ember-concurrency';

/**
 * Create a generic String-Union key -> value mapping constraint
 * where the string union must be keys on the map and values
 */
type StringUnionKeyToValue<U extends string> = { [K in U]: K };

/**
 * Generic String Enum enforcing the keys on an object are found in
 * enum and value is of type V
 */
type StringEnumKeyToEnumValue<T extends string, V> = { [K in T]: V };

/**
 * Describes the index signature for a generic object
 * @interface IObject
 */
interface IObject<T> {
  [K: string]: T;
}

/**
 * A task can be used instead of a promise in some cases, but a task
 * has the advantage of being cancellable. See ember-concurrency.
 */
type PromiseOrTask<T> = PromiseLike<T> | TaskInstance<T> | undefined;

export { StringUnionKeyToValue, StringEnumKeyToEnumValue, IObject, PromiseOrTask };
