import {
  SearchResultDataItem,
  ISearchDataWithMetadata,
  IDataModelEntitySearchResult
} from '@datahub/data-models/types/entity/search';
import { IFacetCounts, IFacetsCounts } from '@datahub/data-models/types/entity/facets';
import { IAggregationMetadata } from '@datahub/shared/types/search/entity';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

/**
 * Computes the search result item's index within the full list of results
 */
export const searchResultItemIndex = <T>({
  itemsPerPage,
  page,
  pageIndex
}: Pick<IDataModelEntitySearchResult<T>, 'page' | 'itemsPerPage'> & { pageIndex: number }): number =>
  itemsPerPage * (page - 1) + pageIndex + 1;

/**
 * Returns a mapping iteratee that takes a search result data element to an object containing a reference to the result item
 * and result item metadata
 */
export const withResultMetadata = <T>(
  metadataOptions: Pick<IDataModelEntitySearchResult<T>, 'page' | 'itemsPerPage'>
): ((data: T, pageIndex: number) => ISearchDataWithMetadata<T>) => (
  data: SearchResultDataItem<T>,
  pageIndex: number
): ISearchDataWithMetadata<T> => {
  const { page, itemsPerPage } = metadataOptions;
  // The result's position in the full list of results returned for the search query
  const resultPosition = searchResultItemIndex({ pageIndex, itemsPerPage, page });
  const meta: ISearchDataWithMetadata<DataModelEntity>['meta'] = {
    resultPosition
  };

  return {
    data,
    meta
  };
};

/**
 * Will merge original facet counts returned by API with previous selected values. This may happen
 * when a filter is selected by default but the result does not contain that value for example:
 * Search result:
 * {
 *  status: {
 *    unpublished: 3
 *  }
 * }
 *
 * Facet Selected:
 * status: ['published']
 *
 * Merge result:
 * {
 *  status: {
 *    unpublished: 3,
 *    published: 0
 *  }
 * }
 *
 * Now the UI can render the checkbox for published with a count of 0
 *
 * @param facetCounts
 * @param previousSelections
 */
export const mergeFacetCountsWithSelections = (
  facetCounts: IFacetsCounts,
  previousSelections: Record<string, Array<string>> = {}
): IFacetsCounts => {
  return Object.keys(previousSelections).reduce((facetCounts: IFacetsCounts, key: string): IFacetsCounts => {
    const withMissingKey = facetCounts[key]
      ? facetCounts
      : {
          ...facetCounts,
          [key]: {}
        };
    const valuesSelected = previousSelections[key];
    const selectedValuesWithNumericCount = valuesSelected.reduce(
      (facetCountValues, value): IFacetCounts =>
        facetCountValues[value]
          ? facetCountValues
          : {
              ...facetCountValues,
              [value]: 0
            },
      withMissingKey[key]
    );
    return {
      ...facetCounts,
      [key]: selectedValuesWithNumericCount
    };
  }, facetCounts);
};

/**
 * Transform api aggregation meta into a more suitable UI structure
 * @param metas entity aggregation meta coming from api
 */
export const searchResultMetasToFacetCounts = (
  metas: Array<IAggregationMetadata>,
  previousSelections: Record<string, Array<string>> = {}
): IFacetsCounts => {
  const facetCounts: IFacetsCounts = metas.reduce(
    (facets: IFacetsCounts, meta: IAggregationMetadata): IFacetsCounts => {
      const facet: IFacetCounts = facets[meta.name] || {};
      return {
        ...facets,
        [meta.name]: {
          ...facet,
          ...meta.aggregations
        }
      };
    },
    {}
  );
  return mergeFacetCountsWithSelections(facetCounts, previousSelections);
};
