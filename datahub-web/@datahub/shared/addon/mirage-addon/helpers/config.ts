import { ApiStatus } from '@datahub/utils/api/shared';
import { setCSRFCookieToken } from '@datahub/shared/mirage-addon/helpers/csrf-protection';
import { HandlerFunction, Schema } from 'ember-cli-mirage';

/**
 * Returns a config object for the config endpoint
 * @param {object} config the config table / factory object
 * @return {{status: ApiStatus, config: object}}
 */
export const getConfig: HandlerFunction = function(schema: Schema) {
  // Set the cookie for csrf protection
  setCSRFCookieToken();
  const configs = schema.applicationCfgs || {};

  return {
    status: ApiStatus.OK,
    config: this.serialize(configs.first())
  };
};
