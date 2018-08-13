import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';

export const getDatasetHealth = function(this: IFunctionRouteHandler, { db: healths }: any) {
  return {
    health: this.serialize(healths[0])
  };
};
