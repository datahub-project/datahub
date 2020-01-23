import { ApiVersion, getApiRoot } from '@datahub/utils/api/shared';

/**
 * Composes the url to the list endpoint, will pass-through the api version if provided
 * @param {ApiVersion} [version] the version of the api to fetch against
 * @return {string}
 */
export const getListUrlRoot = (version?: ApiVersion): string => `${getApiRoot(version)}/list`;
