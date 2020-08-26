import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageDatasetCoreSchema } from '@datahub/datasets-core/types/vendor/mirage-for-datasets';
import { IDatasetSchema } from '@datahub/metadata-types/types/entity/dataset/scehma';

// TODO: [META-8403] Needs to be expanded into properly getting schemas for certain dataset test scenarios,
// but this will be sufficient for the current situation of testing basic compliance table flow

/**
 * This handler is used in the mirage route config to handle get requests for a dataset schema.
 */
export const getDatasetSchema = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema
): { schema: IDatasetSchema } {
  const dsSchema: IDatasetSchema = {
    keySchema: null,
    lastModified: 1548806346860,
    rawSchema: '',
    schemaless: false,
    columns: [...schema.db.datasetSchemaColumns]
  };

  return { schema: this.serialize(dsSchema) };
};
