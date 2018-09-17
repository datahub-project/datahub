import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';

const getDatasetDownstreams = function(this: IFunctionRouteHandler, { lineageViews }: { lineageViews: any }) {
  return this.serialize(lineageViews.all());
};

export { getDatasetDownstreams };
