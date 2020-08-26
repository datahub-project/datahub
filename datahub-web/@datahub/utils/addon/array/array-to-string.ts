/**
 * Converts an string array into a human readable string, eg: [1, 2, 3] => '1, 2 or 3';
 * @param {Array<string>} array input array of strings
 * @param {string} separator separator for more than 2 items
 * @param {string} lastSeparator separator for the last 2 items in the list
 */
export const arrayToString = (array: Array<string>, separator = ', ', lastSeparator = ' or '): string => {
  let result = '';
  if (array.length > 1) {
    result = `${array.slice(0, array.length - 1).join(separator)}${lastSeparator}`;
  }
  return `${result}${array[array.length - 1]}`;
};
