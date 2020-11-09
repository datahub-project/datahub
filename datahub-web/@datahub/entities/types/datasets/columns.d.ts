import { ApiStatus } from '@datahub/utils/api/shared';
import { IDatasetSchemaColumn } from '@datahub/metadata-types/types/entity/dataset/schema';

/**
 * Describes the interface that extends a DatasetColumn with a string
 */
export interface IDatasetColumnWithHtmlComments extends IDatasetSchemaColumn {
  commentHtml: string;
}

/**
 * Describes a dataset column GET request response
 */
export interface IDatasetColumnsGetResponse {
  status: ApiStatus;
  columns?: Array<IDatasetSchemaColumn>;
  message?: string;
  schemaless: boolean;
}
