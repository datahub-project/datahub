import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetView = function(this: IFunctionRouteHandler, { datasetViews }: { datasetViews: any }) {
  return {
    dataset: this.serialize(datasetViews.first()),
    status: ApiStatus.OK
  };
};

export { getDatasetView };
