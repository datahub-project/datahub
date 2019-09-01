import { debounce } from '@ember/runloop';

/**
 * Parameters for the callback fn
 */
export interface IDebounceAndMemoizeAsyncQueryCallbackParams<J = unknown> {
  // string to autocomplete, for example: 'da' for 'datahub'
  query: string;
  // cache key to save the result,
  // this is useful for apis that have different params with different responses
  // for example: 'name:da' or 'description:da'
  cacheKey: string;
  // Params that you may need to call api
  requestParams: J;
  // optional query threshold. If input length is greater than this threshold, then
  // api call will be performed. Otherwise default value is returned.
  // If this value is not found in the request, it will fallback to default value of debouncedQueryCallback
  queryLengthThreshold?: number;
}
/**
 * Asynchronously invokes the supplied queryCallback function and will debounce repeated invocations
 * within the supplied delay window. Also memoizes the successful execution of the callback
 * @template T the type asynchronously resolved by the invocation of the queryCallback
 * @param queryCallback the function to process the query string
 * @param defaultResponse if the query string length threshold is not met or exceeded the value to resolve with
 * @param [delayMs=200] the delay in ms the queryCallback should be debounced with
 * @param [queryLengthThreshold=2] minimum number of characters the query string must have to invoke the callback
 * @returns a proxy function to the supplied queryCallback, with matching interface
 */
export const debounceAndMemoizeAsyncQuery = <T, J, Q extends (...args: Array<J>) => Promise<T>>(
  queryCallback: Q,
  options: {
    defaultResponse?: T;
    delayMs?: number;
    queryLengthThreshold?: number;
  } = {}
): ((a: IDebounceAndMemoizeAsyncQueryCallbackParams<Parameters<typeof queryCallback>>) => Promise<T | undefined>) => {
  const { defaultResponse = undefined, delayMs = 200, queryLengthThreshold: queryLengthThresholdParent = 3 } = options;
  // Aliases the arguments type of the queryCallback function, to allow for an easier and more readable reference
  // to be trafficked through application call sites
  type QueryCallbackArgs = Parameters<Q>;

  // Memoization cache for queries and the response of a successful invocation for that query,
  const queryResponseCache: Record<string, T> = {};

  /**
   * Asynchronously waits for the execution of queryCallback and if successful invokes the supplied resolver with the value,
   * otherwise invokes the rejection function with the captured error object
   * @param {(value?: T) => void} resolve a Promise executor resolve function to invoke when the queryCallback is successfully run
   * @param {(reason?: any) => void} reject a Promise executor rejection function to run when the queryCallback invocation throws
   * @param {...QueryCallbackArgs} arguments list to be spread back into the queryCallback function
   *
   * @param QueryCallbackArgs.1 query
   * @param QueryCallbackArgs.2 value
   * @param args optional spread arguments
   * @returns {Promise<void>}
   */

  const runQueryCallback = async (
    resolve: (value?: T) => void,
    reject: (reason?: unknown) => void,
    params: IDebounceAndMemoizeAsyncQueryCallbackParams<QueryCallbackArgs>
  ): Promise<void> => {
    try {
      resolve(await queryCallback(...params.requestParams));
    } catch (e) {
      reject(e);
    }
  };

  /**
   * Asynchronously debounce the query callback function and resolves or rejects when the invocation of
   * queryCallback is complete
   * @template T the type of value queryCallback resolves with
   * @param {...QueryCallbackArgs} queryCallbackArgs arguments supplied to function queryCallback
   * @returns {Promise<T>}
   */
  const debouncedQueryCallback = <T>(
    queryCallbackArgs: IDebounceAndMemoizeAsyncQueryCallbackParams<QueryCallbackArgs>
  ): Promise<T | undefined> =>
    new Promise((resolve, reject) => {
      debounce(null, runQueryCallback, resolve, reject, queryCallbackArgs, delayMs);
    });
  return async (args: IDebounceAndMemoizeAsyncQueryCallbackParams<QueryCallbackArgs>): Promise<T | undefined> => {
    const { query, cacheKey: _cacheKey, queryLengthThreshold } = args;
    // the cache key is serialized from the query string and the callback function's toString method
    const cacheKey = `${_cacheKey}:${queryCallback}`;
    const threshold = queryLengthThreshold === undefined ? queryLengthThresholdParent : queryLengthThreshold;
    // if the character length of the string query is greater than the threshold
    // then perform the query by invoking the supplied callback
    if (query.length >= threshold) {
      // if the cache contains the previously seen query and serialized function, read from the cache
      // otherwise, invoke the callback and write to the cache before returning
      return queryResponseCache.hasOwnProperty(cacheKey)
        ? queryResponseCache[cacheKey]
        : (queryResponseCache[cacheKey] = (await debouncedQueryCallback(args)) as T);
    }

    // otherwise, resolve with the default response
    return defaultResponse;
  };
};
