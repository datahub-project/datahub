import DatasetSchema from '@datahub/data-models/entity/dataset/modules/schema';
import { IDatasetSchemaColumn } from '@datahub/metadata-types/types/entity/dataset/scehma';

/**
 * Allows us to abstract away most of the logic to generate a mock list of schema fields and allows
 * us to create these fields easily in a customizable way through the tests and mock application
 * @param {Array<string>} fieldNames - field names to generate for the mocked schema fields
 */
export const generateDatasetSchemaFields = (fieldNames: Array<string> = []): Array<IDatasetSchemaColumn> => {
  return fieldNames.map(
    (name): IDatasetSchemaColumn => ({
      comment: '',
      commentCount: 0,
      dataType: 'pretendType',
      distributed: false,
      fieldName: name,
      fullFieldPath: name,
      id: null,
      indexed: false,
      nullable: false,
      parentSortID: 0,
      partitioned: false,
      sortID: 0,
      treeGridClass: null
    })
  );
};

/**
 * Allows us to abstract away most of the logic to generate a mock schema class instance and allows
 * us to create these fields easily in a customizable way through the tests and mock application
 * @param {Array<IDatasetSchemaColumn>} columns - list of schema field objects to be added to the
 *  mocked schema
 */
export const generateDatasetSchema = (columns: Array<IDatasetSchemaColumn> = []): DatasetSchema => {
  return new DatasetSchema({
    columns,
    schemaless: false,
    rawSchema: 'testSchema',
    keySchema: 'testSchema'
  });
};
