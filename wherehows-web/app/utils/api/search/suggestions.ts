import { getJSON } from 'wherehows-web/utils/api/fetcher';
import { getApiRoot } from 'wherehows-web/utils/api/shared';
import buildUrl from 'wherehows-web/utils/build-url';
import { ISuggestionsApi, ISuggestionsResponse } from 'wherehows-web/typings/app/search/suggestions';

/**
 * Build suggestions url
 * @param {ISuggestionApi} params api contract
 * @return {string} return a url with get paramenters attached
 */
export const suggestionsUrl = (params: ISuggestionsApi): string => {
  return buildUrl(`${getApiRoot()}/autocomplete/datasets`, params);
};

/**
 * Fetch suggestions from API
 * @param {ISuggestionApi} params api contract
 * @return {Promise<ISuggestionsResponse>} returns a promise of the suggestions api response
 */
export const readSuggestions = (params: ISuggestionsApi): Promise<ISuggestionsResponse> =>
  getJSON<ISuggestionsResponse>({ url: suggestionsUrl(params) });
