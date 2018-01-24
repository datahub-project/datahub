import { ApiVersion } from 'wherehows-web/utils/api';
import { getApiRoot } from 'wherehows-web/utils/api/shared';

/**
 * Composes the url to the list endpoint, will pass-through the api version if provided
 * @param {ApiVersion} [version] the version of the api to fetch against
 * @return {string}
 */
const getListUrlRoot = (version?: ApiVersion) => `${getApiRoot(version)}/list`;

export { getListUrlRoot };
