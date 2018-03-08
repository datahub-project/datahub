import { IReadDatasetsOptionBag } from 'wherehows-web/typings/api/datasets/dataset';
import { ApiVersion, getApiRoot } from 'wherehows-web/utils/api/shared';
import { encodeForwardSlash } from 'wherehows-web/utils/validators/urn';
import buildUrl from 'wherehows-web/utils/build-url';

/**
 * Defines the endpoint for datasets
 * @type {string}
 */
export const datasetsUrlRoot = (version: ApiVersion) => `${getApiRoot(version)}/datasets`;

/**
 * Constructs a url to get a dataset with a given id
 * @param {number} id the id of the dataset
 * @return {string} the dataset url
 */
export const datasetUrlById = (id: number): string => `${datasetsUrlRoot('v1')}/${id}`;

/**
 * Composes a url to get a specific dataset by urn
 * @param {string} urn
 * @returns {string}
 */
export const datasetUrlByUrn = (urn: string): string => `${datasetsUrlRoot('v2')}/${urn}`;

/**
 * Composes the datasets count url from a given platform and or prefix if provided
 * @param {Partial<IReadDatasetsOptionBag>} [{ platform, prefix }={}]
 * @returns {string}
 */
export const datasetsCountUrl = ({ platform, prefix }: Partial<IReadDatasetsOptionBag> = {}): string => {
  const urlRoot = `${datasetsUrlRoot('v2')}/count`;

  if (platform && prefix) {
    return `${urlRoot}/platform/${platform}/prefix/${encodeForwardSlash(prefix)}`;
  }

  if (platform) {
    return `${urlRoot}/platform/${platform}`;
  }

  return urlRoot;
};

/**
 * Composes the datasets url using the platform and prefix if one is provided
 * @param {IReadDatasetsOptionBag} { platform, prefix }
 * @returns {string}
 */
export const datasetsUrl = ({ platform, prefix, start = 0 }: IReadDatasetsOptionBag): string => {
  const urlRoot = datasetsUrlRoot('v2');
  let url = urlRoot;

  if (platform) {
    url = `${urlRoot}/platform/${platform}`;

    if (prefix) {
      url = `${urlRoot}/platform/${platform}/prefix/${encodeForwardSlash(prefix)}`;
    }
  }

  return buildUrl(url, 'start', `${start}`);
};
