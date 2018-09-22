import { arrayReduce } from 'wherehows-web/utils/array';

/**
 * For each given query param, creates an object with `refreshModel` set to true
 * @param {Array<string>} [params=[]]
 * @returns {Record<string, { refreshModel: true }>}
 */

export const refreshModelForQueryParams = (params: Array<string> = []): Record<string, { refreshModel: true }> =>
  arrayReduce((queryParams, param: string) => ({ ...queryParams, [param]: { refreshModel: true } }), {})(params);
