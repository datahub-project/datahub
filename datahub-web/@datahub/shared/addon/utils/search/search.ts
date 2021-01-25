import { getJSON } from '@datahub/utils/api/fetcher';
import { toRestli, fromRestli } from 'restliparams';
import {
  IFacetsSelectionsMap,
  IFacetsSelectionsArray,
  IFacetSelections
} from '@datahub/data-models/types/entity/facets';
import { ISearchEntityApiParams, IEntitySearchResult } from '@datahub/shared/types/search/entity';
import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';
import buildUrl from '@datahub/utils/api/build-url';

/**
 * Converts IFacetsSelectionsMap into IFacetsSelectionsArray
 * @param selections
 */
export const toFacetSelectionsArray = (selections: IFacetsSelectionsMap): IFacetsSelectionsArray =>
  Object.keys(selections).reduce((newSelections: IFacetsSelectionsArray, key): IFacetsSelectionsArray => {
    const selectionsArray = Object.keys(selections[key]).reduce((arr, selKey): Array<string> => {
      if (selections[key][selKey] && selKey) {
        return [...arr, selKey];
      }
      return arr;
    }, []);
    if (selectionsArray.length > 0) {
      newSelections[key] = selectionsArray;
    }
    return newSelections;
  }, {});

/**
 * Converts IFacetsSelectionsArray into IFacetsSelectionsMap
 * @param selections
 */
export const toFacetSelectionsMap = (selections: IFacetsSelectionsArray): IFacetsSelectionsMap =>
  Object.keys(selections).reduce((newSelections: IFacetsSelectionsMap, key): IFacetsSelectionsMap => {
    newSelections[key] = selections[key].reduce((obj: IFacetSelections, selKey: string): IFacetSelections => {
      obj[selKey] = true;
      return obj;
    }, {});
    return newSelections;
  }, {});

/**
 * Transform IFacetsSelectionsMap into this string: (source:List(hive, hdfs))
 * @param selections
 */
export const facetToParamUrl = (selections: IFacetsSelectionsMap): string => {
  return toRestli(toFacetSelectionsArray(selections));
};

/**
 * Transform (source:List(hive, hdfs)) into IFacetsSelectionsMap
 * @param selections
 */
export const facetFromParamUrl = (value = ''): IFacetsSelectionsMap => {
  return toFacetSelectionsMap(fromRestli(value));
};

/**
 * Build search url
 */
export const searchUrl = (
  { facets, ...params }: ISearchEntityApiParams,
  version: ApiVersion = ApiVersion.v1
): string => {
  return buildUrl(`${getApiRoot(version)}/search`, {
    ...params,
    ...facets
  });
};

/**
 * TODO META-11675 rename, remove V2 as there is no V1
 * Queries the search endpoint using the search params
 * @param {ISearchApiParams} params
 * @returns {Promise<ISearchResponse>}
 */
export const readSearchV2 = <T>(params: ISearchEntityApiParams): Promise<IEntitySearchResult<T>> =>
  getJSON<IEntitySearchResult<T>>({ url: searchUrl(params, ApiVersion.v2) });
