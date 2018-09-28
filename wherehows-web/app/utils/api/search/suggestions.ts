import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { getApiRoot } from 'wherehows-web/utils/api/shared';
import buildUrl from 'wherehows-web/utils/build-url';
import { ISuggestionsApi, ISuggestionsResponse } from 'wherehows-web/typings/app/search/suggestions';

/**
 * Build search url
 */
export const suggestionsUrl = (params: ISuggestionsApi): string => {
  return buildUrl(`${getApiRoot()}/autocomplete/datasets`, params);
};

/**
 * Fetch Search from API
 */
export const readSuggestions = (params: ISuggestionsApi) =>
  getJSON<ISuggestionsResponse>({ url: suggestionsUrl(params) });
