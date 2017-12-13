import { IFunctionRouteHandler } from 'wherehows-web/typings/ember-cli-mirage';
import { ApiStatus } from 'wherehows-web/utils/api/shared';

/**
 * Returns a config object for the config endpoint
 * @param {object} config the config table / factory object
 * @return {{status: ApiStatus, config: object}}
 */
const getConfig = function(this: IFunctionRouteHandler, { configs }: { configs: any }) {
  return {
    status: ApiStatus.OK,
    config: this.serialize(configs.first())
  };
};

export { getConfig };
