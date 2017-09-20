import { datasetUrlById } from 'wherehows-web/utils/api/datasets/shared';
import {
  IDatasetColumn,
  IDatasetColumnsGetResponse,
  IDatasetColumnWithHtmlComments
} from 'wherehows-web/typings/api/datasets/columns';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { ApiStatus } from 'wherehows-web/utils/api';
import { arrayMap } from 'wherehows-web/utils/array-map';

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
 * Maps a IDatasetColumn to an object containing markdown comments, if the dataset has a comment attribute
 * @param {IDatasetColumn} column
 * @return {IDatasetColumnWithHtmlComments | IDatasetColumn}
 */
const columnWithHtmlComment = (column: IDatasetColumn): IDatasetColumnWithHtmlComments | IDatasetColumn => {
  const { comment } = column;
  // TODO: DSS-6122 Refactor global function reference to marked
  return comment ? { ...column, commentHtml: window.marked(comment).htmlSafe() } : column;
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
 * Takes a list of IDatasetColumn and returns an array of IDatasetColumn or IDatasetColumnWithHtmlComments
 * @type {(array: Array<IDatasetColumn>) => Array<IDatasetColumnWithHtmlComments | IDatasetColumn>}
 */
const columnsWithHtmlComments = arrayMap(columnWithHtmlComment);

/**
 * Takes a list of IDatasetColumn / IDatasetColumn with html comments and pulls the dataType and and fullFieldPath (as fieldName) attributes
 * @type {(array: Array<IDatasetColumn | IDatasetColumnWithHtmlComments>) => Array<Pick<IDatasetColumn, "dataType" | "fieldName">>}
 */
const columnDataTypesAndFieldNames = arrayMap(columnDataTypeAndFieldName);

/**
 * Gets the dataset columns for a dataset with the id specified
 * @param {number} id the id of the dataset
 * @return {Promise<Array<IDatasetColumn>>}
 */
const readDatasetColumns = async (id: number): Promise<Array<IDatasetColumn>> => {
  const { status, columns } = await getJSON<IDatasetColumnsGetResponse>({ url: datasetColumnUrlById(id) });

  // Returns an empty list if the status is ok but the columns is falsey
  if (status === ApiStatus.OK) {
    return columns || [];
  }

  throw new Error(datasetColumnsException);
};

export { readDatasetColumns, columnDataTypesAndFieldNames, columnsWithHtmlComments };
