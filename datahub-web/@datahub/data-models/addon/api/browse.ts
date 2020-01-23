import { getJSON, cacheApi } from '@datahub/utils/api/fetcher';
import { ApiVersion, getApiRoot } from '@datahub/utils/api/shared';
import buildUrl from '@datahub/utils/api/build-url';
import { IBrowseParams, IBrowseResponse, IBrowsePathParams } from '@datahub/data-models/types/entity/browse';

/**
 * URL for browse
 * @param version api version
 */
export const browseUrlRoot = (version: ApiVersion): string => `${getApiRoot(version)}/browse`;

/**
 * URL for browse paths
 * @param version api version
 */
export const browsePathUrlRoot = (version: ApiVersion): string => `${getApiRoot(version)}/browsePaths`;

/**
 * Will return the string url for the browse api
 * @param params GET paramaters required for this API call, see IBrowseParams type
 */
const browseUrl = <T>(params: IBrowseParams<T>): string => {
  const urlRoot = browseUrlRoot(ApiVersion.v2);
  return buildUrl(`${urlRoot}`, params);
};

/**
 * Build GET request for browse
 * @param params entity type and URN
 */
const browsePathUrl = (params: IBrowsePathParams): string => {
  const urlRoot = browsePathUrlRoot(ApiVersion.v2);
  return buildUrl(`${urlRoot}`, params);
};
/**
 * Will fetch browse information for a specific path and entity.
 * Will return a paginated return of elements and a fixed set of groups/folders.
 * Note that groups won't be affected by pagination
 */
export const readBrowse = cacheApi(
  <T>(params: IBrowseParams<T>): Promise<IBrowseResponse> => {
    const url = browseUrl(params);
    return getJSON<IBrowseResponse>({ url });
  }
);

/**
 * Read the path to reach to the specified entity
 */
export const readBrowsePath = cacheApi(
  (params: IBrowsePathParams): Promise<Array<string>> => {
    const url = browsePathUrl(params);
    return getJSON<Array<string>>({ url });
  }
);
