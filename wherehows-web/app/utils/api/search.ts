import { getApiRoot, ApiStatus } from 'wherehows-web/utils/api/shared';
import buildUrl from 'wherehows-web/utils/build-url';
import { getJSON } from 'wherehows-web/utils/api/fetcher';

export interface ISearchApiParams {
  keyword: string;
  category: string;
  source: string;
  page: number;
  [key: string]: any;
}

export interface ISearchResponse {
  status: ApiStatus;
  result: {
    keywords: string;
    data: Array<any>;
  };
}

export const searchUrl = (params: ISearchApiParams): string => buildUrl(`${getApiRoot()}/search`, params);

export const readSearch = (params: ISearchApiParams) => getJSON<ISearchResponse>({ url: searchUrl(params) });
