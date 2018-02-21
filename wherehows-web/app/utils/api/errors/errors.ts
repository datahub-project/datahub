import { apiErrorStatusMessage } from 'wherehows-web/constants/errors/errors';

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
    throw new Error(msg);
  }

  return response.json();
};

export { throwIfApiError };
