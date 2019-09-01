import { getJSON } from '@datahub/utils/api/fetcher';

/**
 * Describes the type for map of urn endpoint names to functions
 */
export type UrnEndpointUrlMap<T extends string> = Record<T, (urn: string) => string>;

/**
 * Returns a curried function that requests JSON from an endpoint by a urn
 * It receives a mapping of jsonEndpoints which is a key value pair, where the key references an endpoint
 * and the value is a function that returns the url to the endpoint
 * @template T types that are assignable to the type UrnEndpointUrlMap<string>
 * @template K the set of properties K in T
 * @param {T} jsonEndpoints instance of type T
 * @returns {<R>(urn: string, endpoint: K) => Promise<R>}
 */
//TODO
// eslint-disable-next-line
export const readJsonFromEndpoint = <T extends UrnEndpointUrlMap<string>, K extends keyof T>(jsonEndpoints: T) => <R>(
  urn: string,
  endpoint: K
): Promise<R> => getJSON<R>({ url: jsonEndpoints[endpoint](urn) });
