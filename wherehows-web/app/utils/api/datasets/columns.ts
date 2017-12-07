import { datasetUrlById } from 'wherehows-web/utils/api/datasets/shared';
import {
  IDatasetColumn,
  IDatasetColumnsGetResponse,
  IDatasetColumnWithHtmlComments
} from 'wherehows-web/typings/api/datasets/columns';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { ApiStatus } from 'wherehows-web/utils/api';
import { arrayMap } from 'wherehows-web/utils/array';

// TODO:  DSS-6122 Create and move to Error module

/**
 * default exception string for columns api
 * @type {string}
 */
const datasetColumnsException = 'An error occurred with the columns api';

/**
 * Constructs a dataset column url using a dataset id
 * @param {number} id the id of the dataset
 * @return {string}
 */
const datasetColumnUrlById = (id: number): string => `${datasetUrlById(id)}/columns`;

/**
 * Maps an object with a column prop to an object containing markdown comments, if the dataset has a comment attribute
 * @template T 
 * @param {T} objectWithComment
 * @return {T & {commentHtml: string} | {} & T}
 */
const augmentWithHtmlComment = <T extends { comment: string }>(objectWithComment: T) => {
  const { comment } = objectWithComment;
  // TODO: DSS-6122 Refactor global function reference to marked
  // not using spread operator here: https://github.com/Microsoft/TypeScript/issues/10727
  // current ts version: 2.5.3
  return Object.assign({}, objectWithComment, comment && { commentHtml: window.marked(comment).htmlSafe() });
};

/**
 * Picks out the dataset dataType and fullFieldPath (as fieldName) properties
 * @param {string} dataType string representing the data type of the field
 * @param {string} fullFieldPath the path of the field on this dataset
 * @return {Pick<IDatasetColumn, "dataType" | "fieldName">}
 */
const columnDataTypeAndFieldName = ({
  dataType,
  fullFieldPath
}: IDatasetColumn | IDatasetColumnWithHtmlComments): Pick<IDatasetColumn, 'dataType' | 'fieldName'> => ({
  dataType,
  fieldName: fullFieldPath
});

/**
 * Takes a list of objects with comments and returns an array of objects with comments or html comments
 * @type {(array: Array<T extends { comment: string } & Object>) => Array<T | T extends { commentHtml: string }>}
 */
const augmentObjectsWithHtmlComments = arrayMap(augmentWithHtmlComment);

/**
 * Takes a list of IDatasetColumn / IDatasetColumn with html comments and pulls the dataType and and fullFieldPath (as fieldName) attributes
 * @type {(array: Array<IDatasetColumn | IDatasetColumnWithHtmlComments>) => Array<Pick<IDatasetColumn, "dataType" | "fieldName">>}
 */
const columnDataTypesAndFieldNames = arrayMap(columnDataTypeAndFieldName);

/**
 * Gets the dataset columns for a dataset with the id specified and the schemaless flag
 * @param {number} id the id of the dataset
 * @return {(Promise<{schemaless: boolean; columns: Array<IDatasetColumn>}>)}
 */
const readDatasetColumns = async (id: number): Promise<{ schemaless: boolean; columns: Array<IDatasetColumn> }> => {
  const { status, columns = [], schemaless, message = datasetColumnsException } = await getJSON<
    IDatasetColumnsGetResponse
  >({
    url: datasetColumnUrlById(id)
  });

  // Returns an empty list if the status is ok but the columns is falsey
  if (status === ApiStatus.OK) {
    return { schemaless, columns };
  }

  throw new Error(message);
};

export { readDatasetColumns, columnDataTypesAndFieldNames, augmentObjectsWithHtmlComments, datasetColumnUrlById };
