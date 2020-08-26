import { isBlank } from '@ember/utils';
import { isObject } from '@datahub/utils/validators/object';
import { decode, encode } from '@datahub/utils/api/encode-decode-uri-component-with-space';

/**
 * Construct a url by appending a query pair (?key=value | &key=value) to a base url and
 *   encoding the query value in the pair
 * @param baseUrl the base or original url that will be appended with a query string
 * @param queryParamOrMap a map of query keys to values or a single query param key
 * @param queryValue if a queryParam is supplied, then a queryValue can be expected
 * @param useEncoding flag indicating if the query values should be encoded
 * @returns {string}
 */
function buildUrl(baseUrl: string, queryParamOrMap?: {}, useEncoding?: boolean): string;
function buildUrl(baseUrl: string, queryParamOrMap?: string, queryValue?: string, useEncoding?: boolean): string;
function buildUrl(
  baseUrl: string,
  queryParamOrMap: string | Record<string, unknown> = {},
  queryValue?: string | boolean,
  useEncoding = true
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
    paramMap = queryParamOrMap;

    if (typeof queryValue === 'boolean') {
      useEncoding = queryValue;
    }
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

    if (useEncoding) {
      // Malformed URL will cause decodeURIComponent to throw
      //   handle and encode queryValue in such instance
      try {
        // Check if queryValue is already encoded,
        //   otherwise encode queryValue before composing url
        //   e.g. if user directly enters query in location bar
        if (decode(paramValue as string) === paramValue) {
          paramValue = encode(paramValue);
        }
      } catch (err) {
        if (err instanceof URIError) {
          paramValue = encode(paramValue as string);
        }

        throw err;
      }
    }

    return `${url}${separator}${paramKey}=${paramValue}`;
  }, baseUrl);
}

export default buildUrl;
