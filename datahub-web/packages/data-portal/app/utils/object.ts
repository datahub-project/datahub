import { arrayReduce } from '@datahub/utils/array/index';
import { isObject } from '@datahub/utils/validators/object';

/**
 * Aliases the exclusion / diff conditional type that specifies that an object
 * contains properties from T, that are not in K
 * From T pick a set of properties that are not in K
 * @alias
 */
export type Omit<T, K> = Pick<T, Exclude<keyof T, K>>;

/**
 * Aliases a record with properties of type keyof T and null values
 * Make all values in T null
 * @template T type to nullify
 * @alias
 */
export type Nullify<T> = Record<keyof T, null>;

/**
 * Checks that an object has it own enumerable props
 * @param {any} object the object to the be tested
 */
export const hasEnumerableKeys = (object: object | unknown): boolean =>
  Boolean(isObject(object) && Object.keys(object).length);

/**
 * Non mutative object attribute deletion. Removes the specified keys from a copy of the object and returns the copy.
 * @template T the object type to drop keys from
 * @template K the keys to be dropped from the object
 * @param {T} o
 * @param {Array<K extends keyof T>} droppedKeys
 * @return {Pick<T, Exclude<keyof T, K extends keyof T>>}
 */
export const omit = <T, K extends keyof T>(o: T, droppedKeys: Array<K>): Omit<T, K> => {
  const partialResult = Object.assign({}, o);

  return arrayReduce((partial: T, key: K) => {
    delete partial[key];
    return partial;
  }, partialResult)(droppedKeys);
};

/**
 * Extracts keys from a source to a new object
 * @template T the object to select keys from
 * @param {T} o the source object
 * @param {Array<K extends keyof T>} pickedKeys
 * @return {Select<T extends object, K extends keyof T>}
 */
export const pick = <T, K extends keyof T>(o: T, pickedKeys: Array<K>): Pick<T, K> =>
  arrayReduce(
    (partial: T, key: K): Pick<T, K> =>
      pickedKeys.includes(key) ? Object.assign(partial, { [key]: o[key] }) : partial,
    {} as T
  )(pickedKeys);

/**
 * Creates an object of type T with a set of properties of type null
 * @template T the type of the source object
 * @template K union of literal types of properties
 * @param {T} o instance of T to be set to null
 * @returns {Nullify<T>}
 */
export const nullify = <T, K extends keyof T>(o: T): Nullify<T> => {
  const nullObj = {} as Nullify<T>;
  const keys = Object.keys(o) as Array<K>;
  return arrayReduce((nullObj, key: K) => Object.assign(nullObj, { [key]: null }), nullObj)(keys);
};
