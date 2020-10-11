import { ApiVersion, getApiRoot } from '@datahub/utils/api/shared';
import { getJSON } from '@datahub/utils/api/fetcher';
import { encodeUrn } from '@datahub/utils/validators/urn';

/**
 * Constructs the Dataset url root endpoint
 * @param {ApiVersion} version the version of the api applicable to retrieve the Dataset
 * @returns {string}
 */
const datasetUrlRoot = (version: ApiVersion): string => `${getApiRoot(version)}/datasets`;

/**
 * Constructs the url for a dataset identified by the provided string urn
 * @param {string} urn the urn to use in querying for dataset entity
 * @returns {string}
 */
export const datasetUrlByUrn = (urn: string): string => `${datasetUrlRoot(ApiVersion.v2)}/${encodeUrn(urn)}`;

/**
 * Constructs the url for a dataset identified by the provided string urn
 * @param {string} urn the urn to use in querying for dataset entity
 * @returns {string}
 */
export const datasetV1UrlByUrn = (urn: string): string => `${datasetUrlRoot(ApiVersion.v1)}/${encodeUrn(urn)}`;

/**
 * Reads a dataset entity from api
 */
export const readDataset = (urn: string): Promise<Com.Linkedin.Dataset.Dataset> =>
  getJSON({ url: datasetUrlByUrn(urn) });
