/**
 * For each given query param, creates an object with `refreshModel` set to true
 * @param {Array<string>} [params=[]]
 * @returns {{}}
 */
export const refreshModelQueryParams = (params: Array<string> = []): {} =>
  params.reduce((queryParams, param) => ({ ...queryParams, [param]: { refreshModel: true } }), {});
