import { ApiVersion, getApiRoot } from '@datahub/utils/api/shared';
import { IDatasetEntity } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';
import { getJSON } from '@datahub/utils/api/fetcher';
import { encodeUrn } from '@datahub/utils/validators/urn';

/**
 * Constructs the Feature url root endpoint
 * @param {ApiVersion} version the version of the api applicable to retrieve the Feature
 * @returns {string}
 */
export const datasetUrlRoot = (version: ApiVersion): string => `${getApiRoot(version)}/datasets`;

/**
 * Constructs the url for a dataset identified by the provided string urn
 * @param {string} urn the urn to use in querying for dataset entity
 * @returns {string}
 */
export const datasetUrlByUrn = (urn: string): string => `${datasetUrlRoot(ApiVersion.v2)}/${encodeUrn(urn)}`;

/**
 * Queries the Feature endpoint with the urn provided to retrieve entity information
 * @param {string} urn
 * @returns {Promise<IDatasetEntity>}
 */
export const readDataset = (urn: string): Promise<IDatasetEntity> =>
  getJSON({ url: datasetUrlByUrn(urn) }).then(({ dataset }): IDatasetEntity => dataset);
