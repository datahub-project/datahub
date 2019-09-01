import { IFunctionRouteHandler } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { ApiStatus } from '@datahub/utils/api/shared';

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
