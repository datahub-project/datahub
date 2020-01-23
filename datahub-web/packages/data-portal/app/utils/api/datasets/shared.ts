import { ApiVersion, getApiRoot } from 'wherehows-web/utils/api/shared';
import buildUrl from 'wherehows-web/utils/build-url';
import { encodeForwardSlash } from '@datahub/utils/validators/urn';
import { IReadDatasetsOptionBag } from '@datahub/data-models/types/entity/dataset';
import { datasetUrlRoot } from '@datahub/data-models/api/dataset/dataset';

/**
 * Base url / root for metrics endpoints
 * @param {ApiVersion} version
 * @returns {string}
 */
export const metricsUrlRoot = (version: ApiVersion): string => `${getApiRoot(version)}/metrics`;

/**
 * Constructs a url to get a dataset with a given id
 * @param {number} id the id of the dataset
 * @return {string} the dataset url
 */
export const datasetUrlById = (id: number): string => `${datasetUrlRoot(ApiVersion.v1)}/${id}`;

/**
 * Composes a url to get a specific dataset by urn
 * @param {string} urn
 * @returns {string}
 */
export const datasetUrlByUrn = (urn: string): string => `${datasetUrlRoot(ApiVersion.v2)}/${urn}`;

/**
 * Composes the datasets url using the platform and prefix if one is provided
 * @param {IReadDatasetsOptionBag} { platform, prefix }
 * @returns {string}
 */
export const datasetsUrl = ({ platform, prefix, start = 0 }: IReadDatasetsOptionBag): string => {
  const urlRoot = datasetUrlRoot(ApiVersion.v2);
  let url = urlRoot;

  if (platform) {
    url = `${urlRoot}/platform/${platform}`;

    if (prefix) {
      url = `${urlRoot}/platform/${platform}/prefix/${encodeForwardSlash(prefix)}`;
    }
  }

  return buildUrl(url, 'start', `${start}`);
};
