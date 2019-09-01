import { IEntityLinkAttrs } from '@datahub/data-models/types/entity/shared';
import { IFacetsCounts } from '@datahub/data-models/types/entity/facets';
import { DataModelEntityInstance } from '@datahub/data-models/entity/entity-factory';
import { ArrayElement } from '@datahub/utils/types/array';

/**
 * standard search results that can be consumed by the ui
 */
export interface IDataModelEntitySearchResult<T> {
  // Search result returned elements
  data: Array<T>;
  // Starting index of the list of search results
  start: number;
  // Number of returned results
  count: number;
  // Items to be rendered per page of dataset result
  itemsPerPage: number;
  // The current page of the search results with datasets
  page: number;
  // The total number of pages that can be rendered for this result, should match Math.ceil(count / itemsPerPage)
  totalPages: number;
  // Aggregation results for the search, broken down by facet properties and values to aggregation value
  facets: IFacetsCounts;
}

/**
 * Aliases the Array item found in the search results data attribute
 * @type SearchResultDataItem
 */
export type SearchResultDataItem<T> = ArrayElement<IDataModelEntitySearchResult<T>['data']>;

/**
 * Defines the interface for a SearchResult metadata object
 * @export
 * @interface ISearchResultMetadata
 */
export interface ISearchResultMetadata<T> {
  entityLink: IEntityLinkAttrs;
  resultPosition: number;
  // Interim approach to provide secondary actions components with a required DataModelEntityInstance reference
  // In the ideal case search results should be reified with the class
  instance: DataModelEntityInstance | null;
}

/**
 * Defines the interface for an object containing a data  attribute with SearchResultDataItem and metadata properties
 * @export
 * @interface ISearchDataWithMetadata
 */
export interface ISearchDataWithMetadata<T> {
  data: T;
  meta: ISearchResultMetadata<T>;
}
