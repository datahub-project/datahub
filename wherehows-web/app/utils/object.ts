/**
 * Checks if a type is an object
 * @param {any} candidate the entity to check
 */
const isObject = (candidate: any): candidate is object =>
  candidate && Object.prototype.toString.call(candidate) === '[object Object]';

/**
 * Checks if an object type is deeply equal to another
 * @param {object} objectA the first object to compare
 * @param {object} objectB the second object to compare against
 * @returns {boolean}
 */
const objectDeepEqual = (objectA: object, objectB: object): boolean => {
  return isObject(objectB) && isObject(objectA) && JSON.stringify(objectA) === JSON.stringify(objectB);
};

/**
 * Checks that an object has it own enumerable props
 * @param {Object} object the object to the be tested
 * @return {boolean} true if enumerable keys are present
 */
const hasEnumerableKeys = (object: object): boolean => isObject(object) && !!Object.keys(object).length;

export { isObject, hasEnumerableKeys, objectDeepEqual };
