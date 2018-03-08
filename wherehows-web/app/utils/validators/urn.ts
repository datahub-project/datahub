import { assert } from '@ember/debug';
import { DatasetPlatform, Fabric } from 'wherehows-web/constants';

/**
 * Matches a url string with a `urn` query. urn query with letters or underscore segment of any length greater
 *   than 1 followed by colon and 3 forward slashes and a segment containing letters, {, }, _ or /, or none
 *   The value following the urn key is retained
 * @type {RegExp}
 */
const datasetUrnRegexWH = /([a-z_-]+):\/{3}([a-z0-9_\-/{}.]*)/i;

/**
 * Matches a urn string that follows the pattern captures, the comma delimited platform, segment and fabric
 * e.g urn:li:dataset:(urn:li:dataPlatform:PLATFORM,SEGMENT,FABRIC)
 * @type {RegExp}
 */
const datasetUrnRegexLI = /urn:li:dataset:\(urn:li:dataPlatform:([\w-]+),([\w.\-\/{}]+),(\w+)\)/;

/**
 * Matches urn's that occur in flow urls
 * @type {RegExp}
 */
const specialFlowUrnRegex = /(?:\?urn=)([a-z0-9_\-/{}\s]+)/i;

/**
 * Checks if a string matches the datasetUrnRegexWH
 * @param {string} candidateUrn
 * @returns {boolean}
 */
const isWhUrn = (candidateUrn: string): boolean => datasetUrnRegexWH.test(String(candidateUrn));

/**
 * Checks if a string matches the datasetUrnRegexLI
 * @param {string} candidateUrn
 * @returns {boolean}
 */
const isLiUrn = (candidateUrn: string): boolean => datasetUrnRegexLI.test(String(candidateUrn));

/**
 * Asserts that a provided string matches the urn pattern above
 * @param {string} candidateUrn the string to test on
 */
const isUrn = (candidateUrn: string) => isLiUrn(candidateUrn) || isWhUrn(candidateUrn);

/**
 * Extracts the platform string from the candidate urn string
 * @param {string} candidateUrn the urn string with leading platform identifier
 * @returns {string | void}
 */
const getPlatformFromUrn = (candidateUrn: string) => {
  const matches = datasetUrnRegexWH.exec(candidateUrn);

  if (matches) {
    const [, platform] = matches;
    return platform.toUpperCase();
  }
};

/**
 * Converts a WH URN format to a LI URN format
 * @param {string} whUrn
 * @return {string}
 */
const convertWhUrnToLiUrn = (whUrn: string): string => {
  assert(`Expected ${whUrn} to be in the WH urn format`, isWhUrn(whUrn));
  const [, platform, path] = datasetUrnRegexWH.exec(whUrn)!;

  return buildLiUrn(<DatasetPlatform>platform, path);
};

/**
 * Takes a dataset platform, path, and fabric to produce an liUrn
 * @param {DatasetPlatform} platform
 * @param {string} path
 * @param {Fabric} [fabric=Fabric.Prod]
 * @return {string}
 */
const buildLiUrn = (platform: DatasetPlatform, path: string, fabric: Fabric = Fabric.Prod): string => {
  const formattedPath = convertWhDatasetPathToLiPath(platform, path);
  return `urn:li:dataset:(urn:li:dataPlatform:${platform},${formattedPath},${fabric})`;
};

/**
 * Converts a path from WH urn format, replace forward slash with periods in non DatasetPlatform.HDFS cases,
 * add leading forward slash if platform is DatasetPlatform.HDFS
 * @param {DatasetPlatform} platform
 * @param {string} path
 * @return {string}
 */
const convertWhDatasetPathToLiPath = (platform: DatasetPlatform, path: string): string => {
  if (String(platform).toLowerCase() === DatasetPlatform.HDFS) {
    return path.charAt(0) === '/' ? path : `/${path}`;
  }

  return path.replace(/\//g, '.');
};

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
const encodeForwardSlash = (urn: string): string => urn.replace(/\//g, encodeURIComponent('/'));

/**
 * Replaces encoded slashes with /
 * @param {string} urn
 * @return {string}
 */
const decodeForwardSlash = (urn: string): string => urn.replace(encodedSlashRegExp, decodeURIComponent('/'));

/**
 * Replaces occurrences of / with the encoded counterpart in a urn string
 * @param {string} urn
 * @return {string}
 */
const encodeUrn = (urn: string): string => encodeForwardSlash(urn);

/**
 * Replaces encoded occurrences of / with the string /
 * @param {string} urn
 * @return {string}
 */
const decodeUrn = (urn: string): string => decodeForwardSlash(urn);

export default isUrn;

export {
  datasetUrnRegexWH,
  datasetUrnRegexLI,
  isWhUrn,
  isLiUrn,
  buildLiUrn,
  specialFlowUrnRegex,
  getPlatformFromUrn,
  convertWhUrnToLiUrn,
  convertWhDatasetPathToLiPath,
  encodeForwardSlash,
  encodeUrn,
  decodeUrn
};
