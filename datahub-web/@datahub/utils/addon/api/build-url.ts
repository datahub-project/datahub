import { isBlank } from '@ember/utils';
import { isObject } from '@datahub/utils/validators/object';

/**
 * Construct a url by appending a query pair (?key=value | &key=value) to a base url and
 *   encoding the query value in the pair
 * @param baseUrl the base or original url that will be appended with a query string
 * @param queryParamOrMap a map of query keys to values or a single query param key
 * @param queryValue if a queryParam is supplied, then a queryValue can be expected
 * @returns {string}
 */
function buildUrl(baseUrl: string, queryParamOrMap?: {}): string;
function buildUrl(baseUrl: string, queryParamOrMap?: string, queryValue?: string): string;
function buildUrl(
  baseUrl: string,
  queryParamOrMap: string | Record<string, unknown> = {},
  queryValue?: string | boolean
): string {
  if (!baseUrl) {
    return '';
  }

  if (!queryParamOrMap) {
    return baseUrl;
  }

  let paramMap: Record<string, unknown> = {};

  // queryParamOrMap is a string then, reify paramMap object with supplied value
  if (typeof queryParamOrMap === 'string') {
    paramMap = {
      [queryParamOrMap]: queryValue
    };
  }

  if (isObject(queryParamOrMap)) {
    paramMap = queryParamOrMap as Record<string, unknown>;
  }

  return Object.keys(paramMap).reduce((url: string, paramKey: string): string => {
    // If the query string already contains the initial question mark append
    //   kv-pair with ampersand
    const separator = String(url).includes('?') ? '&' : '?';
    let paramValue = paramMap[paramKey];

    if (Array.isArray(paramValue)) {
      paramValue = paramValue.toString();
    }

    if (isBlank(paramValue)) {
      return url;
    }

    return `${url}${separator}${paramKey}=${paramValue}`;
  }, baseUrl);
}

export default buildUrl;
