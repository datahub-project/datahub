import { getJSON, cacheApi } from '@datahub/utils/api/fetcher';
import { ApiVersion, getApiRoot } from '@datahub/utils/api/shared';
import buildUrl from '@datahub/utils/api/build-url';
import { IBrowseParams, IBrowseResponse } from '@datahub/data-models/types/entity/browse';

export const browseUrlRoot = (version: ApiVersion): string => `${getApiRoot(version)}/browse`;

/**
 * Will return the string url for the browse api
 * @param params GET paramaters required for this API call, see IBrowseParams type
 */
const browseUrl = <T>(params: IBrowseParams<T>): string => {
  const urlRoot = browseUrlRoot(ApiVersion.v2);
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
