import { IDatasetColumn } from 'datahub-web/typings/api/datasets/columns';

/**
 * Describes the properties on a dataset schema object
 * @interface
 */
export interface IDatasetSchema {
  schemaless: boolean;
  rawSchema: null | string;
  keySchema: null | string;
  lastModified?: number;
  columns: Array<IDatasetColumn>;
}

/**
 * Describes the properties on a response to a request for dataset schema
 * @interface
 */
export interface IDatasetSchemaGetResponse {
  schema: IDatasetSchema;
}
