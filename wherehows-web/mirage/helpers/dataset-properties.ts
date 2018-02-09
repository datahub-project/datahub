import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

const getDatasetProperties = function(this: IFunctionRouteHandler) {
  return {
    properties: {},
    status: ApiStatus.OK
  };
};

export { getDatasetProperties };
