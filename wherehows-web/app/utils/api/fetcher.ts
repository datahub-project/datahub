import fetch from 'fetch';

/**
 * Describes the attributes on the fetch configuration object
 */
interface FetchConfig {
  url: string;
  headers?: { [key: string]: string };
}

/**
 * Conveniently gets a JSON response using the fetch api
 * @param {FetchConfig} config
 * @return {Promise<T>}
 */
const getJSON = <T>(config: FetchConfig): Promise<T> => {
  const fetchConfig = {
    method: 'GET',
    Accept: 'application/json',
    'Content-Type': 'application/json',
    ...(config.headers || {})
  };

  return fetch(config.url, fetchConfig).then<T>(response => response.json());
};

/**
 * Requests the headers from a resource endpoint
 * @param {FetchConfig} config
 * @return {Promise<Headers>>}
 */
const getHeaders = async (config: FetchConfig): Promise<Headers> => {
  const fetchConfig = {
    method: 'HEAD',
    ...(config.headers || {})
  };
  const { ok, headers, statusText } = await fetch(config.url, fetchConfig);

  if (ok) {
    return headers;
  }

  throw new Error(statusText);
};

export { getJSON, getHeaders };
