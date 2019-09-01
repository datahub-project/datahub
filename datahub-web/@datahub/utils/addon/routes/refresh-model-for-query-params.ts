import { reduce } from 'lodash';

/**
 * Shorthand to reduce visual complexity of code below
 */
type QueryParam = Record<string, { refreshModel: true }>;

/**
 * For each given query param, creates an object with `refreshModel` set to true
 * @param {Array<string>} [params=[]]
 * @returns {Record<string, { refreshModel: true }>}
 */
export const refreshModelForQueryParams = (params: Array<string> = []): QueryParam =>
  reduce(
    params,
    (queryParams: QueryParam, param: string): QueryParam => ({ ...queryParams, [param]: { refreshModel: true } }),
    {}
  );
