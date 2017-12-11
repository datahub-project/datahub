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

export { isObject, hasEnumerableKeys };
