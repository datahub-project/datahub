import { capitalize } from '@ember/string';

/**
 * Takes a string, returns a formatted string
 * @param {string} string
 */
const formatAsCapitalizedStringWithSpaces = (string: string) => capitalize(string.toLowerCase().replace(/[_]/g, ' '));

export { formatAsCapitalizedStringWithSpaces };
