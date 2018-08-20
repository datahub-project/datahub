import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';

const getDatasetCount = function(this: IFunctionRouteHandler, { datasetsCounts }: any) {
  const { count } = this.serialize(datasetsCounts.first());

  return count;
};

export { getDatasetCount };
