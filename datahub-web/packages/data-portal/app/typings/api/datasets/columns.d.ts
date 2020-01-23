import { ApiStatus } from '@datahub/utils/api/shared';

/**
 * Describes a dataset column
 */
export interface IDatasetColumn {
  comment: string;
  commentCount: null | number;
  dataType: string;
  distributed: boolean;
  fieldName: string;
  fullFieldPath: string;
  id: number | null;
  indexed: boolean;
  nullable: boolean;
  parentSortID: number;
  partitioned: boolean;
  sortID: number;
  treeGridClass: null;
}

/**
 * Describes the interface that extends a DatasetColumn with a string
 */
export interface IDatasetColumnWithHtmlComments extends IDatasetColumn {
  commentHtml: string;
}

/**
 * Describes a dataset column GET request response
 */
export interface IDatasetColumnsGetResponse {
  status: ApiStatus;
  columns?: Array<IDatasetColumn>;
  message?: string;
  schemaless: boolean;
}
