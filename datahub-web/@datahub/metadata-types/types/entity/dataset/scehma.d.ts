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
  // Denotes that the dataset contains no explicitly defined schema, e.g. Couchbase
  schemaless: boolean;
  // The raw schema content
  rawSchema: null | string;
  // If dataset has dedicated key schema which is separated from the table or value part, it can be stored here
  keySchema: null | string;
  // Date of last modification for the schema
  lastModified?: number;
  // Flattened/normalized field-level schema definition with fully-qualified field path
  columns?: Array<IDatasetSchemaColumn>;
}
