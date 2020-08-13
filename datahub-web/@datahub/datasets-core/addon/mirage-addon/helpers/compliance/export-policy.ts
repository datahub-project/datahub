import { IFunctionRouteHandler, IMirageRequest } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageDatasetCoreSchema } from '@datahub/datasets-core/types/vendor/mirage-for-datasets';
import { IDatasetExportPolicy } from '@datahub/metadata-types/types/entity/dataset/compliance/export-policy';

/**
 * This handler is used by the mirage route config to handle the get request for dataset export policy
 */
export const getDatasetExportPolicy = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema
): { exportPolicy: IDatasetExportPolicy } {
  const exportPolicy = schema.db.datasetExportPolicies[0];
  return {
    exportPolicy: this.serialize({
      ...exportPolicy,
      modifiedBy: 'catran',
      modifiedTime: 1552531600 * 1000
    })
  };
};

/**
 * This handler is used by the mirage route config to handle the post request for export policy. It overwrites
 * all current export factory policies since we only have 1 at the moment and don't deal with relationships
 */
export const postDatasetExportPolicy = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema,
  req: IMirageRequest
): void {
  const requestBody = JSON.parse(req.requestBody);
  schema.db.datasetExportPolicies.update(requestBody);
};
