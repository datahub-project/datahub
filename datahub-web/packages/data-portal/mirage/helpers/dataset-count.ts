import { IFunctionRouteHandler, IMirageRequest } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageWherehowsDBs } from 'wherehows-web/typings/ember-cli-mirage';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

const getDatasetCount = function(
  this: IFunctionRouteHandler,
  { datasetViews }: IMirageWherehowsDBs,
  request: IMirageRequest<{}, { platform_id: DatasetPlatform }>
): number {
  return datasetViews.where({ platform: request && request.params && request.params.platform_id }).models.length;
};

export { getDatasetCount };
