import { encode, decode } from 'wherehows-web/utils/encode-decode-uri-component-with-space';
import { isBlank } from '@ember/utils';

/**
 * Construct a url by appending a query pair (?key=value | &key=value) to a base url and
 *   encoding the query value in the pair
 * @param {String} baseUrl the base or original url that will be appended with a query string
 * @param {String} queryParam
 * @param {String} queryValue
 * @returns {string}
 */
function buildUrl(): string;
function buildUrl(baseUrl: string, mapParams: Record<string, any>): string;
function buildUrl(baseUrl: string, queryKey: string, queryValue: string): string;
function buildUrl(baseUrl?: string, queryParamOrMap?: string | Record<string, string>, queryValue?: string): string {
  if (!baseUrl) {
    return '';
  }

  if (!queryParamOrMap) {
    return baseUrl;
  }

  let paramMap: { [x: string]: string };
  if (typeof queryParamOrMap === 'string') {
    paramMap = {
      [queryParamOrMap]: queryValue || ''
    };
  } else {
    paramMap = queryParamOrMap;
  }

  return Object.keys(paramMap).reduce((url, paramKey) => {
    // If the query string already contains the initial question mark append
    //   kv-pair with ampersand
    const separator = String(url).includes('?') ? '&' : '?';
    let paramValue = paramMap[paramKey];

    if (isBlank(paramValue)) {
      return url;
    }

    // Malformed URL will cause decodeURIComponent to throw
    //   handle and encode queryValue in such instance
    try {
      // Check if queryValue is already encoded,
      //   otherwise encode queryValue before composing url
      //   e.g. if user directly enters query in location bar
      if (decode(paramValue) === queryValue) {
        paramValue = encode(paramValue);
      }
    } catch (err) {
      if (err instanceof URIError) {
        paramValue = encode(paramValue);
      }

      throw err;
    }

    return `${url}${separator}${paramKey}=${paramValue}`;
  }, baseUrl);
}

export default buildUrl;
