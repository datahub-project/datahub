import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';

const getDatasetView = function(this: IFunctionRouteHandler, { datasetViews }: { datasetViews: any }) {
  return {
    dataset: this.serialize(datasetViews.first())
  };
};

export { getDatasetView };
