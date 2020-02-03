import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageDatasetCoreSchema } from '@datahub/datasets-core/types/vendor/mirage-for-datasets';
import { IDatasetEntity } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';

/**
 * Mirage response for the dataset entity getter API request, used in the mirage router file for a more
 * clean approach
 */
export const getDatasetEntity = function(
  this: IFunctionRouteHandler,
  schema: IMirageDatasetCoreSchema
): { dataset: IDatasetEntity } {
  const datasetInfo = schema.db.datasetViews[0];
  return { dataset: this.serialize(datasetInfo) };
};
