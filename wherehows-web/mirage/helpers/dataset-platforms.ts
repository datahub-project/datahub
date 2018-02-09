import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetPlatforms = function(this: IFunctionRouteHandler) {
  return {
    platforms: [],
    status: ApiStatus.OK
  };
};

export { getDatasetPlatforms };
