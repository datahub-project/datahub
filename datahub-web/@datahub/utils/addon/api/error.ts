import { ApiResponseStatus, isNotFoundApiError } from '@datahub/utils/api/shared';

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

  /**
   *Creates an instance of ApiError, expects the status type, ApiResponseStatus, as the first argument which corresponds to the available HTTP status codes
   * @param {ApiResponseStatus} status The status code of the response from the targeted API endpoint
   * @param {string} message An error message potentially describing the reason for the error
   * @param {...Array<string>} args Additional optional string arguments that may be passed into a javascript Error constructor
   */
  constructor(readonly status: ApiResponseStatus, message: string, ...args: Array<string>) {
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
export const apiErrorStatusMessage = (status: ApiResponseStatus): string => {
  const statusMessages: Record<number, string> = {
    [ApiResponseStatus.NotFound]: 'Could not find the requested resource',
    [ApiResponseStatus.InternalServerError]: 'An error occurred with the server'
  };

  return statusMessages[status];
};
/**
 * Wraps a Response object, pass through json response if no api error,
 * otherwise raise exception with error message
 * @template T
 * @param {Response} response the fetch Response object to check for an error state
 * @param {(response: Response) => Promise<T>} cb if response is not an api error, then invoke callback with response object
 */
export const throwIfApiError = async <T>(response: Response, cb: (response: Response) => Promise<T>): Promise<T> => {
  const { status, ok } = response;

  if (!ok) {
    const { msg = apiErrorStatusMessage(status) } = await response.json();
    throw new ApiError(status, msg);
  }

  return cb(response);
};

/**
 * Can be plugged into the .catch() chain of our fetcher as a callback, will return a default value
 * if the API has returned a 404, otherwise will let the error be thrown. Helpful when we want a
 * clean way to give a default and handle this error
 * @param {any} value - value to return if API is a not found error
 * @example
 * const pokemon = await getJSON({ url: '/pokemon/ash' }).catch(getDefaultIfNotFoundError([]));
 */
export const getDefaultIfNotFoundError = <T>(value: T): ((e: Error) => T) => (e: Error): T => {
  if (isNotFoundApiError(e)) {
    return value;
  }
  throw e;
};
