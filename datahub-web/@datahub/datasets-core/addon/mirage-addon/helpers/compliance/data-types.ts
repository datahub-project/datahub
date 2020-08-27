import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageDatasetCoreSchema } from '@datahub/datasets-core/types/vendor/mirage-for-datasets';
import { IComplianceDataType } from '@datahub/metadata-types/types/entity/dataset/compliance-data-types';

/**
 * This handler is used by the mirage route config to handle the get request for dataset compliance data types
 */
export const getComplianceDataTypes = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema
): { complianceDataTypes: Array<IComplianceDataType> } {
  return { complianceDataTypes: this.serialize(schema.complianceDataTypes.all()) };
};
