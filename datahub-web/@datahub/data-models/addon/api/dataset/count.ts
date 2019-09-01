import { cacheApi, getJSON } from '@datahub/utils/api/fetcher';
import { IReadDatasetsOptionBag } from '@datahub/data-models/types/entity/dataset';
import { encodeForwardSlash } from '@datahub/utils/validators/urn';
import { datasetUrlRoot } from '@datahub/data-models/api/dataset/dataset';
import { ApiVersion } from '@datahub/utils/api/shared';

/**
 * Composes the datasets count url from a given platform and or prefix if provided
 * @param {Partial<IReadDatasetsOptionBag>} [{ platform, prefix }={}]
 * @returns {string}
 */
export const datasetsCountUrl = ({ platform, prefix }: Partial<IReadDatasetsOptionBag> = {}): string => {
  const urlRoot = `${datasetUrlRoot(ApiVersion.v2)}/count`;

  if (platform && prefix) {
    return `${urlRoot}/platform/${platform}/prefix/${encodeForwardSlash(prefix)}`;
  }

  if (platform) {
    return `${urlRoot}/platform/${platform}`;
  }

  return urlRoot;
};

/**
 * Gets the number of datasets, if provided, using the platform and prefix also
 * @param {Partial<IReadDatasetsOptionBag>} { platform, prefix }
 * @returns {Promise<number>}
 */
export const readDatasetsCount = cacheApi(
  ({ platform, prefix }: Partial<IReadDatasetsOptionBag>): Promise<number> => {
    const url = datasetsCountUrl({ platform, prefix });
    return getJSON<number>({ url });
  }
);
