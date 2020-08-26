import { ApiVersion, getApiRoot } from '@datahub/utils/api/shared';

/**
 * Constructs the Feature url root endpoint
 * @param {ApiVersion} version the version of the api applicable to retrieve the Feature
 * @returns {string}
 */
export const featureUrlRoot = (version: ApiVersion): string => `${getApiRoot(version)}/features`;
