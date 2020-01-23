/**
 * Describes the attributes on the fetch configuration object
 * @export
 * @interface IFetchConfig
 */
export interface IFetchConfig {
  url: string;
  headers?: Record<string, string> | Headers;
  data?: object;
}

/**
 * Describes the available options on an option bag to be passed into a fetch call
 * @export
 * @interface IFetchOptions
 */
export interface IFetchOptions {
  method?: string;
  body?: any;
  headers?: object | Headers;
  credentials?: RequestCredentials;
}
