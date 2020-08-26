import { helper } from '@ember/component/helper';
import { isArray } from '@ember/array';

/**
 * Options to modify the basic behavior of our function by allowing us to ignore certain special cases
 * when we don't want to modify
 */
interface IDisplayValueOptions {
  ignoreBoolean?: boolean;
  ignoreArray?: boolean;
}

/**
 * Simple template helper that takes in a number of expected value types and returns a modified version
 * of the value if a special condition is meant. Ex, for arrays of strings or numbers, we want to render
 * a list of the values with comma and space separators. For booleans, we want to say "Yes" instead of
 * true, etc.
 * @param value - passed in value from the template
 */
export function displayValue([value, options = {}]: [unknown, IDisplayValueOptions]): string {
  if (isArray(value) && !options.ignoreArray) {
    return (value as Array<unknown>).join(', ');
  }

  if (typeof value === 'boolean' && !options.ignoreBoolean) {
    return value ? 'Yes' : 'No';
  }

  return `${value}`;
}

export default helper(displayValue);
