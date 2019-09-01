/**
 * Describes a dataset column
 */
export interface IDatasetSchemaColumn {
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
 * Describes the properties on a dataset schema object
 * @interface
 */
export interface IDatasetSchema {
  schemaless: boolean;
  rawSchema: null | string;
  keySchema: null | string;
  lastModified?: number;
  columns: Array<IDatasetSchemaColumn>;
}
