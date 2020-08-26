import { ITrackSearchResultImpressionsParams } from '@datahub/shared/types/tracking/event-tracking';

/**
 * Describes the function parameters for the tracking service method to track entity search.
 *
 * Track entity search consists of 1) Track site search event and 2) Track search result impression event
 * @interface ITrackEntitySearchParams
 */
export interface ITrackEntitySearchParams extends ITrackSiteSearchParams {
  // parameters required to track search result impressions in detail. It provides additional information like `urn` and `absolute position` for each result item
  searchResultImpressionsTrackingParams: ITrackSearchResultImpressionsParams;
}

/**
 * Describes the function parameters for the tracking service method to track site search event
 *
 * @interface ITrackSiteSearchParams
 */
export interface ITrackSiteSearchParams {
  // Search keyword the user searched for
  keyword: string;
  // Search category for search results. If not needed, set to false
  entity: string | false;
  // Number of results on the search results page. Zero indicates a 'No Results Search'. Set to false if not known
  searchCount: number | false;
}
