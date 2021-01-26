import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';
import { IDatasetSchema } from '@datahub/metadata-types/types/entity/dataset/schema';
import { getJSON } from '@datahub/utils/api/fetcher';

/**
 * Returns the url for a dataset schema by urn
 * @param {string} urn
 * @return {string}
 */
const datasetSchemaUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/schema`;

/**
 * Reads the schema for a dataset with the related urn
 * @param {string} urn
 * @return {Promise<IDatasetSchema>}
 */
export const readDatasetSchema = (urn: string): Promise<IDatasetSchema> =>
  getJSON({ url: datasetSchemaUrlByUrn(urn) }).then(({ schema }) => schema);
