import { ApiStatus } from 'wherehows-web/utils/api/shared';

/**
 * Returns a config object for the config endpoint
 * @param {object} config the config table / factory object
 * @return {{status: ApiStatus, config: object}}
 */
const getConfig = ({ config }: { config: object }) => ({
  status: ApiStatus.OK,
  config
});

export {
  getConfig
}
