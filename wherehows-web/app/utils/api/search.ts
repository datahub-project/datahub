import { getApiRoot, ApiStatus } from 'wherehows-web/utils/api/shared';
import buildUrl from 'wherehows-web/utils/build-url';
import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { toRestli, fromRestli } from 'restliparams';

export interface ISearchApiParams {
  keyword: string;
  category: string;
  page: number;
  facets: string;
  [key: string]: any;
}

export interface ISearchResponse {
  status: ApiStatus;
  result: {
    keywords: string;
    data: Array<any>;
  };
}

export interface IFacetSelections {
  [key: string]: boolean;
}

export interface IFacetsSelectionsMap {
  [key: string]: IFacetSelections;
}

export interface IFacetsSelectionsArray {
  [key: string]: Array<string>;
}

export const toFacetSelectionsArray = (selections: IFacetsSelectionsMap): IFacetsSelectionsArray =>
  Object.keys(selections).reduce((newSelections: any, key) => {
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

export const toFacetSelectionsMap = (selections: IFacetsSelectionsArray): IFacetsSelectionsMap =>
  Object.keys(selections).reduce((newSelections: any, key) => {
    newSelections[key] = selections[key].reduce((obj: any, selKey: string) => {
      obj[selKey] = true;
      return obj;
    }, {});
    return newSelections;
  }, {});

export const facetToParamUrl = (selections: IFacetsSelectionsMap) => {
  return toRestli(toFacetSelectionsArray(selections));
};

export const facetFromParamUrl = (value: string = '') => {
  return toFacetSelectionsMap(fromRestli(value));
};

export const searchUrl = ({ facets, ...params }: ISearchApiParams): string => {
  return buildUrl(`${getApiRoot()}/search`, { ...params, ...fromRestli(facets || '') });
};

export const readSearch = (params: ISearchApiParams) => getJSON<ISearchResponse>({ url: searchUrl(params) });
