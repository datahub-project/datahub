import { IReadDatasetsOptionBag } from 'wherehows-web/typings/api/datasets/dataset';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { ApiVersion, getApiRoot } from 'wherehows-web/utils/api/shared';

/**
 * Generates the base url for a platform given a specified ApiVersion
 * @param {ApiVersion} version
 */
export const platformsUrlRoot = (version: ApiVersion) => `${getApiRoot(version)}/platforms`;

/**
 * Composes a url for platforms, uses platform and prefix if provided
 * @param {IReadDatasetsOptionBag} {platform}
 * @param {IReadDatasetsOptionBag} {prefix}
 * @returns {string}
 */
const platformsUrl = ({ platform, prefix }: IReadDatasetsOptionBag): string => {
  const urlRoot = platformsUrlRoot('v2');

  if (platform && prefix) {
    return `${urlRoot}/${platform}/prefix/${prefix}`;
  }

  if (platform) {
    return `${urlRoot}/${platform}`;
  }

  return urlRoot;
};

/**
 * Reads the platforms endpoint and returns a list of platforms or prefixes found
 * @param {IReadDatasetsOptionBag} {platform}
 * @param {IReadDatasetsOptionBag} {prefix}
 * @returns {Promise<Array<string>>}
 */
const readPlatforms = async ({ platform, prefix }: IReadDatasetsOptionBag): Promise<Array<string>> => {
  const url = platformsUrl({ platform, prefix });
  const response = await getJSON<Array<string>>({ url });

  return response || [];
};

export { readPlatforms };
