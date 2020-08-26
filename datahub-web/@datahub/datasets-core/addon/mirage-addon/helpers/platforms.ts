import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageDatasetCoreSchema } from '@datahub/datasets-core/types/vendor/mirage-for-datasets';
import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';

/**
 * This handler is used by the mirage route config to handle the get request for dataset compliance data types
 */
export const getDataPlatforms = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema
): { platforms: Array<IDataPlatform> } {
  return { platforms: this.serialize(schema.db.platforms) };
};
