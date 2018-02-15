/**
 * Matches an a string to an email pattern
 * @type {RegExp}
 */
const emailRegex = /^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/g;

/**
 * Checks that a candidate string is an email
 * @param candidateEmail {string}
 * @return {boolean}
 */
export default (candidateEmail: string): boolean => emailRegex.test(String(candidateEmail));
