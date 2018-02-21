import { IDatasetColumn } from 'wherehows-web/typings/api/datasets/columns';

/**
 * Describes the properties on a dataset schema object
 * @interface
 */
interface IDatasetSchema {
  schemaless: boolean;
  rawSchema: null | string;
  keySchema: null | string;
  columns: Array<IDatasetColumn>;
}

/**
 * Describes the properties on a response to a request for dataset schema
 * @interface
 */
interface IDatasetSchemaGetResponse {
  schema: IDatasetSchema;
}

export { IDatasetSchema, IDatasetSchemaGetResponse };
