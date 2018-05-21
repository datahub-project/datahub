import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';

const getDatasetRetention = function(this: IFunctionRouteHandler, { retentions }: { retentions: any }) {
  return {
    retentionPolicy: this.serialize(retentions.first())
  };
};

export { getDatasetRetention };
