import { IPaginatedResponse } from '@datahub/utils/types/api/response';

/**
 * The aggregation metadata is mapped to properties on the search response as searchResultMetadatas and
 * contains data used mostly for facet parameters in the search page where we aggregate how many results
 * are available for each facet value
 */
interface IAggregationMetadata {
  name: string;
  aggregations: {
    [key: string]: number;
  };
}

/**
 * Backend search expected parameters
 */
export interface ISearchEntityApiParams {
  // user input keywords
  input: string;
  // The actual entity we want to return
  type: string;
  // Index from which to start returning search results. This is for pagination, but based on index
  start?: number;
  // Number of results to return (equivalent to page size) api expected default is 10
  count?: number;
  // Facet params expected
  facets: Record<string, Array<string>>;
  // Aspects that we need to fetch for the entity type
  aspects?: Array<string>;
}

/**
 * Describes the interface for the result property found on the v2 search endpoint response object. V2
 * search can be used to find multiple entities, so our generic type can be used to denote which entity
 * is expected to be in the results
 * @interface IEntitySearchResult
 */
export interface IEntitySearchResult<T> extends IPaginatedResponse<T> {
  // Aggregation results for the search, broken down by facet properties and values to aggregation value
  searchResultMetadatas: Array<IAggregationMetadata>;
}
