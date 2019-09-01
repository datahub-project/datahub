import searchResponse from 'wherehows-web/mirage/fixtures/search-response';
import { ISearchResponse } from 'wherehows-web/typings/api/search/search';

/**
 * Returns the fixture instance of ISearchResponse
 * @returns {ISearchResponse}
 */
export const getSamplePageViewResponse = (): ISearchResponse => searchResponse;
