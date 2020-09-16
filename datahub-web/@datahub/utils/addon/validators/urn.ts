/**
 * Cached RegExp object for a global search of /
 * @type {RegExp}
 */
const encodedSlashRegExp = new RegExp(encodeURIComponent('/'), 'g');
/**
 * Replaces any occurrence of / with the encoded equivalent
 * @param {string} urn
 * @return {string}
 */
export const encodeForwardSlash = (urn: string): string => urn.replace(/\//g, () => encodeURIComponent('/'));

/**
 * Replaces encoded slashes with /
 * @param {string} urn
 * @return {string}
 */
export const decodeForwardSlash = (urn: string): string =>
  urn.replace(encodedSlashRegExp, () => decodeURIComponent('/'));

/**
 * Replaces occurrences of / with the encoded counterpart in a urn string
 * @param {string} urn
 * @return {string}
 */
export const encodeUrn = (urn: string): string => encodeForwardSlash(urn);

/**
 * Replaces encoded occurrences of / with the string /
 * @param {string} urn
 * @return {string}
 */
export const decodeUrn = (urn: string): string => decodeForwardSlash(urn);

/**
 * Stores the encoded URL for the asterisk/wildcard symbol since encodeURIComponent doesn't catch these
 * as a reserved symbol
 * @type {string}
 */
const encodedWildcard = '%2A';

/**
 * Cached RegExp object for a global search of /
 * @type {RegExp}
 */
const encodedWildcardRegExp = new RegExp(encodedWildcard, 'g');

/**
 * Replaces any occurence of * with the encoded equivalent
 * @param {string} urn
 * @return {string}
 */
export const encodeWildcard = (urn: string): string => urn.replace(/\*/g, encodedWildcard);

/**
 * Replaces encoded slashes with /
 * @param {string} urn
 * @return {string}
 */
export const decodeWildcard = (urn: string): string => urn.replace(encodedWildcardRegExp, decodeURIComponent('*'));

/**
 * Will extract the entity type from a urn
 * @param urn
 */
export const extractEntityType = (urn: string): string | undefined => urn.split(':')[2];
