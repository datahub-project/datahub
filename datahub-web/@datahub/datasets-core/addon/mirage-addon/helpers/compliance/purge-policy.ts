import { IFunctionRouteHandler, IMirageRequest } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageDatasetCoreSchema } from '@datahub/datasets-core/types/vendor/mirage-for-datasets';
import { IDatasetRetentionPolicy } from '@datahub/metadata-types/types/entity/dataset/compliance/retention';

/**
 * This handler is used by the mirage route config to handle the get request for dataset export policy
 */
export const getDatasetPurgePolicy = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema
): { retentionPolicy: IDatasetRetentionPolicy } {
  const purgePolicy = schema.db.datasetPurgePolicies[0];
  return {
    retentionPolicy: this.serialize(purgePolicy)
  };
};

/**
 * This handler is used by the mirage route config to handle the post request for export policy. It overwrites
 * all current export factory policies since we only have 1 at the moment and don't deal with relationships
 */
export const postDatasetPurgePolicy = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema,
  req: IMirageRequest
): void {
  const requestBody = JSON.parse(req.requestBody);
  schema.db.datasetPurgePolicies.update(requestBody);
};
