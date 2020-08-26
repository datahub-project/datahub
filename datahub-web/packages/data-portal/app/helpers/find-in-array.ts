import { helper } from '@ember/component/helper';
import { isArray } from '@ember/array';

/**
 * Recreates the Array.find() function where the first value in the array will be returned that strictly
 * matches the given value or passes the predicate function's boolean test
 * @param {Array<T>} list - the list to be traversed
 * @param {primitive value or (item: T) => boolean} valueOrPredicate - the item to test against to find
 * @returns {T}
 */
// TODO: This helper should be moved to nacho utils whenever we can work on importing the open source version
// back into wherehows
export function findInArray<T>([list, valueOrPredicate]: [
  Array<T>,
  number | string | boolean | ((item: T) => boolean)
]): T | undefined {
  if (!isArray(list)) {
    throw new Error('expected first parameter to find-in-array helper to be an array');
  }

  const predicate =
    typeof valueOrPredicate === 'function' ? valueOrPredicate : (item: unknown): boolean => item === valueOrPredicate;

  return list.find(predicate);
}

export default helper(findInArray);
