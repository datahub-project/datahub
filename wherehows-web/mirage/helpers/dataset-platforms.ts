import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';

const getDatasetPlatforms = function(this: IFunctionRouteHandler, { platforms }: { platforms: any }) {
  return {
    platform: this.serialize(platforms.all())
  };
};

export { getDatasetPlatforms };
