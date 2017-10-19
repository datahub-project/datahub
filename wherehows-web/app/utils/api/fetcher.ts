import fetch from 'fetch';

/**
 * Describes the attributes on the fetch configuration object
 */
interface FetchConfig {
  url: string;
  headers?: { [key: string]: string } | Headers;
  data?: object;
}

/**
 * 
 * 
 * @param {FetchConfig} config 
 */
const baseFetchHeaders = (headers: FetchConfig['headers']) => ({
  headers: {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    ...headers
  }
});

/**
 * 
 * 
 * @template T 
 * @param {string} url 
 * @param {object} fetchConfig 
 * @returns {Promise<T>} 
 */
const json = <T>(url: string, fetchConfig: object): Promise<T> =>
  fetch(url, fetchConfig).then<T>(response => response.json());

/**
 * Conveniently gets a JSON response using the fetch api
 * @param {FetchConfig} config
 * @return {Promise<T>}
 */
const getJSON = <T>(config: FetchConfig): Promise<T> => {
  const fetchConfig = { ...baseFetchHeaders(config.headers), method: 'GET' };

  return json<T>(config.url, fetchConfig);
};

/**
 * 
 * 
 * @template T 
 * @param {FetchConfig} config 
 * @returns {Promise<T>} 
 */
const postJSON = <T>(config: FetchConfig): Promise<T> => {
  const fetchConfig = Object.assign(
    config.data && { body: JSON.stringify(config.data) },
    baseFetchHeaders(config.headers),
    { method: 'POST' }
  );

  return json<T>(config.url, fetchConfig);
};

const deleteJSON = <T>(config: FetchConfig): Promise<T> => {
  const fetchConfig = Object.assign(
    config.data && { body: JSON.stringify(config.data) },
    baseFetchHeaders(config.headers),
    { method: 'DELETE' }
  );

  return json<T>(config.url, fetchConfig);
};

const putJSON = <T>(config: FetchConfig): Promise<T> => {
  const fetchConfig = Object.assign(
    config.data && { body: JSON.stringify(config.data) },
    baseFetchHeaders(config.headers),
    { method: 'PUT' }
  );

  return json<T>(config.url, fetchConfig);
};

/**
 * Requests the headers from a resource endpoint
 * @param {FetchConfig} config
 * @return {Promise<Headers>>}
 */
const getHeaders = async (config: FetchConfig): Promise<Headers> => {
  const fetchConfig = {
    ...baseFetchHeaders(config.headers),
    method: 'HEAD'
  };
  const { ok, headers, statusText } = await fetch(config.url, fetchConfig);

  if (ok) {
    return headers;
  }

  throw new Error(statusText);
};

export { getJSON, postJSON, deleteJSON, putJSON, getHeaders };
