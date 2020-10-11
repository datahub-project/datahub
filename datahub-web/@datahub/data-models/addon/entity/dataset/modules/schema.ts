import { IDatasetSchema, IDatasetSchemaColumn } from '@datahub/metadata-types/types/entity/dataset/schema';
import { oneWay } from '@ember/object/computed';

/**
 * Provides a default schema object if the API does not return anything
 */
export const datasetSchemaFactory = (): IDatasetSchema => ({
  schemaless: false,
  rawSchema: null,
  keySchema: null,
  columns: []
});

export default class DatasetSchema {
  /**
   * The raw data that comes from the API layer for the dataset schema
   * @type {IDatasetSchema}
   */
  data: IDatasetSchema;

  /**
   * A pointer to the schemaless field in the original data. This is here to prevent accidental reading and writing
   * of the actual data retrieved from the api layer
   * @type {boolean}
   */
  @oneWay('data.schemaless')
  isSchemaless?: boolean;

  /**
   * A pointer to the rawSchema field in the original data. This is here to prevent accidental reading and writing
   * of the actual data retrieved from the api layer
   * @type {string}
   */
  @oneWay('data.rawSchema')
  rawSchema?: string;

  /**
   * A pointer to the keySchema field in the original data. This is here to prevent accidental reading and writing
   * of the actual data retrieved from the api layer
   * @type {string}
   */
  @oneWay('data.keySchema')
  keySchema?: string;

  /**
   * A pointer to the columns field in the original data. This is here to prevent accidental reading and writing
   * of the actual data retrieved from the api layer, and provide a more descriptive name to the consumer
   * @type {Array<IDatasetSchemaColumn>}
   */
  @oneWay('data.columns')
  schemaFields?: Array<IDatasetSchemaColumn>;

  constructor(data: IDatasetSchema = datasetSchemaFactory()) {
    this.data = data;
  }
}
