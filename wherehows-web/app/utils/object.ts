/**
 * Checks if a type is an object
 * @param {any} candidate the entity to check
 */
const isObject = (candidate: any): candidate is object =>
  candidate && Object.prototype.toString.call(candidate) === '[object Object]';

/**
 * Checks that an object has it own enumerable props
 * @param {any} object the object to the be tested
 * @return {boolean} true if enumerable keys are present
 */
const hasEnumerableKeys = (object: any): boolean => isObject(object) && !!Object.keys(object).length;

/**
 * Function interface for a identity or partial return function
 * @interface IPartialOrIdentityTypeFn
 * @template T
 */
interface IPartialOrIdentityTypeFn<T> {
  (o: T): Partial<T> | T;
}
/**
 * Non mutative object attribute deletion. Removes the specified keys from a copy of the object and returns the copy.
 * @template T the object type to drop keys from
 * @template K the keys to be dropped from the object
 * @param {Array<K>} [droppedKeys=[]] the list of attributes on T to be dropped
 * @returns {IPartialOrIdentityTypeFn<T>}
 */
const fleece = <T, K extends keyof T = keyof T>(droppedKeys: Array<K> = []): IPartialOrIdentityTypeFn<T> => (
  o: T
): Partial<T> | T => {
  const partialResult = Object.assign({}, o);

  return droppedKeys.reduce((partial, key) => {
    delete partial[key];
    return partial;
  }, partialResult);
};

export { isObject, hasEnumerableKeys, fleece };
