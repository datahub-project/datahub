import { encodeForwardSlash } from '@datahub/utils/validators/urn';
import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';
import { getJSON, cacheApi } from '@datahub/utils/api/fetcher';
import { IReadDatasetsOptionBag } from '@datahub/data-models/types/entity/dataset';

/**
 * Generates the base url for a platform given a specified ApiVersion
 * @param {ApiVersion} version
 */
export const platformsUrlRoot = (version: ApiVersion): string => `${getApiRoot(version)}/platforms`;

/**
 * Composes a url for platforms, uses platform and prefix if provided
 * @param {IReadDatasetsOptionBag} {platform}
 * @param {IReadDatasetsOptionBag} {prefix}
 * @returns {string}
 */
const platformsUrl = ({ platform, prefix }: IReadDatasetsOptionBag): string => {
  const urlRoot = platformsUrlRoot(ApiVersion.v2);

  if (platform && prefix) {
    return `${urlRoot}/${platform}/prefix/${encodeForwardSlash(prefix)}`;
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
const readPlatforms = cacheApi(
  ({ platform, prefix }: IReadDatasetsOptionBag): Promise<Array<string>> => {
    const url = platformsUrl({ platform, prefix });
    return getJSON<Array<string>>({ url });
  }
);

export { readPlatforms };
