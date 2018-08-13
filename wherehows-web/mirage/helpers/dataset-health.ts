import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';

export const getDatasetHealth = function(this: IFunctionRouteHandler, { health }: any, request: any) {
  const { dataset_id } = request.params;

  return {
    health: health.filter((score: any) => score.refDatasetUrn === dataset_id)[0] || null
  };
};
