import { ApiStatusNumber } from 'wherehows-web/utils/api/shared';

/**
 * Returns a default msg for a given status
 * @param {ApiStatusNumber} status
 * @returns {string}
 */
const apiErrorStatusMessage = (status: ApiStatusNumber): string =>
  (<{ [prop: number]: string }>{
    [ApiStatusNumber.NotFound]: 'Could not find the requested resource',
    [ApiStatusNumber.InternalServerError]: 'An error occurred with the server'
  })[status];

export { apiErrorStatusMessage };
