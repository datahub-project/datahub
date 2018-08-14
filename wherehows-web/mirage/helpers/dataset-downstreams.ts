import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';

const getDatasetDownstreams = function(this: IFunctionRouteHandler, { datasetViews }: { datasetViews: any }) {
  return this.serialize(datasetViews.all());
};

export { getDatasetDownstreams };
