/**
 * Describes the function parameters for the tracking service method to track site search event
 * @export
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
