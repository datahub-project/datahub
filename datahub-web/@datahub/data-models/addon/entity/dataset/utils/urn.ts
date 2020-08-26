import { datasetPlatformUrnPattern } from '@datahub/metadata-types/utils/entity/dataset/platform/urn';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';

/**
 * Some datasets have slash in their name as prefix as a map for easy exist check
 */
export const PlatformsWithSlash: Partial<Record<DatasetPlatform, true>> = {
  [DatasetPlatform.HDFS]: true,
  [DatasetPlatform.SEAS_HDFS]: true,
  [DatasetPlatform.SEAS_DEPLOYED]: true
};

/**
 * All fabrics in an array format compatible with search
 */
export const fabricsArray: Array<string> = Object.keys(FabricType).map((key: keyof typeof FabricType): string =>
  FabricType[key].toLowerCase()
);

/**
 * Type guard that asserts a string as a dataset fabric
 * @param {string} candidate
 * @return {boolean}
 */
export const isDatasetFabric = (candidate: string): candidate is FabricType =>
  Object.values(FabricType).includes(candidate.toUpperCase() as FabricType);

/**
 * Path segment in a urn. common btw WH and LI formats
 * @type {RegExp}
 */
export const urnPath = /[\w.$\-\/{}+()\s=\*]+/;
/**
 * Matches a url string with a `urn` query. urn query with letters or underscore segment of any length greater
 *   than 1 followed by colon and 3 forward slashes and a segment containing letters, {, }, _ or /, or none
 *   The value following the urn key is retained
 * @type {RegExp}
 */
export const datasetUrnRegexWH = new RegExp(`([a-z_-]+):\/{3}(${urnPath.source})`, 'i');

/**
 * Matches a urn string that follows the pattern captures, the comma delimited platform, segment and fabric
 * e.g urn:li:dataset:(urn:li:dataPlatform:PLATFORM,SEGMENT,FABRIC)
 * @type {RegExp}
 */
export const datasetUrnRegexLI = new RegExp(
  `urn:li:dataset:\\(${datasetPlatformUrnPattern},(${urnPath.source})?,(\\w+)\\)`
);

/**
 * Matches urn's that occur in flow urls
 * @type {RegExp}
 */
export const specialFlowUrnRegex = /(?:\?urn=)([a-z0-9_\-/{}\s]+)/i;

/**
 * Checks if a string matches the datasetUrnRegexWH
 * @param {string} candidateUrn
 * @returns {boolean}
 */
export const isWhUrn = (candidateUrn: string): boolean => datasetUrnRegexWH.test(String(candidateUrn));

/**
 * Checks if a string matches the datasetUrnRegexLI
 * @param {string} candidateUrn
 * @returns {boolean}
 */
export const isLiUrn = (candidateUrn: string): boolean => datasetUrnRegexLI.test(String(candidateUrn));

/**
 * Checks that a string matches the expected valuePatternRegex
 * @param {string} candidate the supplied pattern string
 * @return {boolean}
 */
export const isValidCustomValuePattern = (candidate: string): boolean => !!candidate;

/**
 * Asserts that a provided string matches the urn pattern above
 * @param {string} candidateUrn the string to test on
 */
export const isUrn = (candidateUrn: string): boolean => isLiUrn(candidateUrn) || isWhUrn(candidateUrn);

/**
 * Extracted interface into a new object
 */
export interface IDatasetGetUrnPartsResponse {
  platform: DatasetPlatform;
  prefix: string;
  fabric: FabricType;
}

/**
 * Extracts the constituent parts of a datasystem / dataset urn
 * @param {string} urn
 * @return {({platform: DatasetPlatform | void; prefix: string | void; fabric: Fabric | void})}
 */
export const getDatasetUrnParts = (urn: string): Partial<IDatasetGetUrnPartsResponse> => {
  const match = datasetUrnRegexLI.exec(urn);
  const urnParts: Partial<IDatasetGetUrnPartsResponse> = {
    platform: void 0,
    prefix: void 0,
    fabric: void 0
  };

  if (match) {
    const [, platform, prefix, urnFabric] = match;
    const fabric = String(urnFabric).toUpperCase();

    return {
      ...urnParts,
      platform: platform.toLowerCase() as DatasetPlatform,
      prefix,
      fabric: isDatasetFabric(fabric) ? fabric : void 0
    };
  }

  return urnParts;
};

/**
 * Converts a path from WH urn format, replace forward slash with periods in non DatasetPlatform.HDFS cases,
 * add leading forward slash if platform is DatasetPlatform.HDFS
 * @param {DatasetPlatform} platform
 * @param {string} path
 * @return {string}
 */
export const convertWhDatasetPathToLiPath = (platform: DatasetPlatform, path: string): string => {
  if (PlatformsWithSlash[String(platform).toLowerCase() as DatasetPlatform]) {
    return path.charAt(0) === '/' ? path : `/${path}`;
  }

  return path.replace(/\//g, '.');
};

/**
 * Takes a dataset platform, path, and fabric to produce an liUrn
 * @param {DatasetPlatform} platform
 * @param {string} path
 * @param {Fabric} [fabric=Fabric.Prod]
 * @return {string}
 */
export const buildDatasetLiUrn = (
  platform: DatasetPlatform,
  path = '',
  fabric: FabricType = FabricType.PROD
): string => {
  const formattedPath = convertWhDatasetPathToLiPath(platform, path);
  return `urn:li:dataset:(urn:li:dataPlatform:${platform},${formattedPath},${fabric})`;
};

/**
 * Converts a WH URN format to a LI URN format
 * @param {string} whUrn
 * @return {string}
 */
export const convertWhUrnToLiUrn = (whUrn: string): string => {
  const [, platform = '', path = ''] = datasetUrnRegexWH.exec(whUrn) || [];

  return buildDatasetLiUrn(platform as DatasetPlatform, path);
};

export default isUrn;
