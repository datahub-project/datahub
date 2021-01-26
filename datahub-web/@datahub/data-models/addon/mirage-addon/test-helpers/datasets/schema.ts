import DatasetSchema from '@datahub/data-models/entity/dataset/modules/schema';
import { IDatasetSchemaColumn } from '@datahub/metadata-types/types/entity/dataset/schema';
import { IDatasetSchema } from '@datahub/metadata-types/types/entity/dataset/schema';
import { HandlerFunction, Server } from 'ember-cli-mirage';

/**
 * Allows us to abstract away most of the logic to generate a mock list of schema fields and allows
 * us to create these fields easily in a customizable way through the tests and mock application
 * @param {Array<string>} fieldNames - field names to generate for the mocked schema fields
 */
export const generateDatasetSchemaFields = (
  fieldNames: Array<string> = [],
  server: Server
): Array<IDatasetSchemaColumn> => {
  return fieldNames.map(
    (name): IDatasetSchemaColumn => server.create('datasetSchemaColumn', { fieldName: name, fullFieldPath: name })
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

// TODO: [META-8403] Needs to be expanded into properly getting schemas for certain dataset test scenarios,
// but this will be sufficient for the current situation of testing basic compliance table flow

/**
 * This handler is used in the mirage route config to handle get requests for a dataset schema.
 */
export const getDatasetSchema: HandlerFunction = function(schema): { schema: IDatasetSchema } {
  const columns = this.serialize(schema.db.datasetSchemaColumns);
  const dsSchema: IDatasetSchema = {
    keySchema: null,
    lastModified: 1548806346860,
    rawSchema: '',
    schemaless: false,
    columns: [...columns]
  };

  return { schema: dsSchema };
};
