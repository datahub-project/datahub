import { ApiResponseStatus } from '@datahub/utils/api/shared';

/**
 * Extends the built-in Error class with attributes related to treating non 200 OK responses
 * at the api layer as exceptions
 * @export
 * @class ApiError
 * @extends {Error}
 */
export class ApiError extends Error {
  /**
   * Timestamp of when the exception occurred
   * @readonly
   * @memberof ApiError
   */
  readonly timestamp = new Date();

  constructor(readonly status: ApiResponseStatus, message: string, ...args: Array<any>) {
    super(...[message, ...args]);
    // Fixes downlevel compiler limitation with correct prototype chain adjustment
    // i.e. ensuring this is also `instanceof` subclass
    Object.setPrototypeOf(this, ApiError.prototype);
  }
}

/**
 * Returns a default msg for a given status
 * @param {ApiResponseStatus} status
 * @returns {string}
 */
export const apiErrorStatusMessage = (status: ApiResponseStatus): string =>
  (({
    [ApiResponseStatus.NotFound]: 'Could not find the requested resource',
    [ApiResponseStatus.InternalServerError]: 'An error occurred with the server'
  } as { [prop: number]: string })[status]);

/**
 * Wraps a Response object, pass through json response if no api error,
 * otherwise raise exception with error message
 * @template T
 * @param {Response} response
 * @param {(response: Response) => any} cb if response is not an api error, then invoke callback with response object
 * @returns {Promise<T>}
 */
export const throwIfApiError = async <T>(response: Response, cb: (response: Response) => any): Promise<T> => {
  const { status, ok } = response;

  if (!ok) {
    const { msg = apiErrorStatusMessage(status) } = await response.json();
    throw new ApiError(status, msg);
  }

  return cb(response);
};
