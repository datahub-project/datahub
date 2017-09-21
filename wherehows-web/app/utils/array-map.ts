/**
 * Convenience utility takes a type-safe mapping function, and returns a list mapping function
 * @param {(param: T) => U} mappingFunction maps a single type T to type U
 * @return {(array: Array<T>) => Array<U>}
 */
const arrayMap = <T, U>(mappingFunction: (param: T) => U): ((array: Array<T>) => Array<U>) => {
  return array => array.map(mappingFunction);
};

export { arrayMap };
