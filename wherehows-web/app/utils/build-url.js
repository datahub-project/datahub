import { encode, decode } from 'wherehows-web/utils/encode-decode-uri-component-with-space';

/**
 * Construct a url by appending a query pair (?key=value | &key=value) to a base url and
 *   encoding the query value in the pair
 * @param {String} baseUrl the base or original url that will be appended with a query string
 * @param {String} queryParam
 * @param {String} queryValue
 * @returns {string}
 */
export default (baseUrl, queryParam, queryValue) => {
  // If the query string already contains the initial question mark append
  //   kv-pair with ampersand
  const separator = String(baseUrl).includes('?') ? '&' : '?';

  // Malformed URL will cause decodeURIComponent to throw
  //   handle and encode queryValue in such instance
  try {
    // Check if queryValue is already encoded,
    //   otherwise encode queryValue before composing url
    //   e.g. if user directly enters query in location bar
    if (decode(queryValue) === queryValue) {
      queryValue = encode(queryValue);
    }
  } catch (err) {
    if (err instanceof URIError) {
      queryValue = encode(queryValue);
    }

    throw err;
  }

  return `${baseUrl}${separator}${queryParam}=${queryValue}`;
};
