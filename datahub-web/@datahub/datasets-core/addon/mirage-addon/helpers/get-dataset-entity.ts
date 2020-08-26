import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageDatasetCoreSchema } from '@datahub/datasets-core/types/vendor/mirage-for-datasets';

/**
 * Mirage response for the dataset entity getter API request, used in the mirage router file for a more
 * clean approach
 */
export const getDatasetEntity = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema
): Com.Linkedin.Dataset.Dataset {
  const datasetInfo = schema.db.datasets[0];
  return this.serialize(datasetInfo);
};
