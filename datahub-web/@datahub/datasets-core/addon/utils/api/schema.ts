import { getJSON } from '@datahub/utils/api/fetcher';
import { datasetUrlByUrn } from '@datahub/data-models/api/dataset/dataset';
import { IDatasetSchema } from '@datahub/metadata-types/types/entity/dataset/schema';
import { IDatasetSchemaGetResponse } from '@datahub/datasets-core/types/datasets/schema';

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
const readDatasetSchemaByUrn = async (urn: string): Promise<IDatasetSchema> => {
  let schema: IDatasetSchema;

  try {
    ({ schema } = await getJSON<IDatasetSchemaGetResponse>({ url: datasetSchemaUrlByUrn(urn) }));
  } catch (e) {
    return {
      schemaless: false,
      rawSchema: null,
      keySchema: null,
      columns: []
    };
  }

  return schema;
};

export { readDatasetSchemaByUrn };
