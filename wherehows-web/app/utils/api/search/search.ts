import { getApiRoot } from 'wherehows-web/utils/api/shared';
import buildUrl from 'wherehows-web/utils/build-url';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { toRestli, fromRestli } from 'restliparams';
import {
  IFacetsSelectionsMap,
  IFacetsSelectionsArray,
  IFacetsCounts,
  IFacetSelections
} from 'wherehows-web/typings/app/search/facets';
import { ISearchResponse, ISearchApiParams } from 'wherehows-web/typings/api/search/search';
/**
 * Convert backend static structure into a dynamic facet count structure
 */
export const facetToDynamicCounts = (result: ISearchResponse['result']): IFacetsCounts => {
  return Object.keys(result).reduce((counts: IFacetsCounts, key) => {
    if (key.indexOf('groupby') === 0) {
      counts[key.replace('groupby', '')] = result[key];
    }
    return counts;
  }, {});
};
/**
 * Converts IFacetsSelectionsMap into IFacetsSelectionsArray
 * @param selections
 */
export const toFacetSelectionsArray = (selections: IFacetsSelectionsMap): IFacetsSelectionsArray =>
  Object.keys(selections).reduce((newSelections: IFacetsSelectionsArray, key) => {
    const selectionsArray = Object.keys(selections[key]).reduce((arr, selKey) => {
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
  Object.keys(selections).reduce((newSelections: IFacetsSelectionsMap, key) => {
    newSelections[key] = selections[key].reduce((obj: IFacetSelections, selKey: string) => {
      obj[selKey] = true;
      return obj;
    }, {});
    return newSelections;
  }, {});
/**
 * Transform IFacetsSelectionsMap into this string: (source:List(hive, hdfs))
 * @param selections
 */
export const facetToParamUrl = (selections: IFacetsSelectionsMap) => {
  return toRestli(toFacetSelectionsArray(selections));
};
/**
 * Transform (source:List(hive, hdfs)) into IFacetsSelectionsMap
 * @param selections
 */
export const facetFromParamUrl = (value: string = '') => {
  return toFacetSelectionsMap(fromRestli(value));
};
/**
 * Build search url
 */
export const searchUrl = ({ facets, ...params }: ISearchApiParams): string => {
  return buildUrl(`${getApiRoot()}/search`, { ...params, ...fromRestli(facets || '') });
};
/**
 * Fetch Search from API
 */
export const readSearch = (params: ISearchApiParams) => getJSON<ISearchResponse>({ url: searchUrl(params) });
