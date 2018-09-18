import { ApiStatus } from 'wherehows-web/utils/api';

/**
 * Backend search expected parameters
 */
export interface ISearchApiParams {
  keyword: string;
  category: string;
  page: number;
  facets: string;
  [key: string]: any;
}

/**
 * Backend search expected response
 */
export interface ISearchResponse {
  status: ApiStatus;
  result: {
    keywords: string;
    data: Array<any>;
    [key: string]: any;
  };
}
