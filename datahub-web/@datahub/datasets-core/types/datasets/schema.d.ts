import { IDatasetSchema } from '@datahub/metadata-types/types/entity/dataset/schema';
/**
 * Describes the properties on a response to a request for dataset schema
 * @interface
 */
export interface IDatasetSchemaGetResponse {
  schema: IDatasetSchema;
}
