import { ApiResponseStatus } from 'wherehows-web/utils/api/shared';

/**
 * Returns a default msg for a given status
 * @param {ApiResponseStatus} status
 * @returns {string}
 */
const apiErrorStatusMessage = (status: ApiResponseStatus): string =>
  (<{ [prop: number]: string }>{
    [ApiResponseStatus.NotFound]: 'Could not find the requested resource',
    [ApiResponseStatus.InternalServerError]: 'An error occurred with the server'
  })[status];

export { apiErrorStatusMessage };
