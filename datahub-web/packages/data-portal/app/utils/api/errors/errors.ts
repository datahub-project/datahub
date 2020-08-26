import { ApiResponseStatus } from '@datahub/utils/api/shared';
import { apiErrorStatusMessage } from '@datahub/utils/api/error';

/**
 * Extends the built-in Error class with attributes related to treating non 200 OK responses
 * at the api layer as exceptions
 * @class ApiError
 * @extends {Error}
 */
class ApiError extends Error {
  /**
   * Timestamp of when the exception occurred
   * @readonly
   * @memberof ApiError
   */
  readonly timestamp = new Date();

  constructor(readonly status: ApiResponseStatus, message: string, ...args: Array<string | undefined>) {
    super(...[message, ...args]);
    // Fixes downlevel compiler limitation with correct prototype chain adjustment
    // i.e. ensuring this is also `instanceof` subclass
    Object.setPrototypeOf(this, ApiError.prototype);
  }
}

/**
 * Wraps a Response object, pass through json response if no api error,
 * otherwise raise exception with error message
 * @template T
 * @param {Response} response
 * @returns {Promise<T>}
 */
const throwIfApiError = async <T>(response: Response): Promise<T> => {
  const { status, ok } = response;

  if (!ok) {
    const { msg = apiErrorStatusMessage(status) } = await response.json();
    throw new ApiError(status, msg);
  }

  return response.json();
};

export { throwIfApiError, ApiError };
