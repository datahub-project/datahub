import { IFunctionRouteHandler, IMirageRequest } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { IMirageWherehowsDBs } from 'wherehows-web/typings/ember-cli-mirage';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';

export const getDatasetView = function(
  this: IFunctionRouteHandler,
  { datasetViews }: IMirageWherehowsDBs,
  request: IMirageRequest<{}, { identifier: string }>
): { dataset: IDatasetView } {
  return {
    dataset: this.serialize(datasetViews.where({ uri: request.params!.identifier }).models[0])
  };
};
