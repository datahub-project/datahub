/**
 * Describes the parameters for trackSiteSearch event type in Piwik
 * @export
 * @interface IPiwikTrackSiteSearchParams
 */
export interface IPiwikTrackSiteSearchParams {
  // Search keyword the user searched for
  keyword: string;
  // Search category for search results. If not needed, set to false
  entity: string | false;
  // Number of results on the search results page. Zero indicates a 'No Results Search'. Set to false if not known
  searchCount: number | false;
}

/**
 * Describes the interface for trackPiwikSiteSearch return type
 * @export
 * @interface ITrackPiwikSiteSearchReturnFn
 */
export interface ITrackPiwikSiteSearchReturnFn {
  (props: IPiwikTrackSiteSearchParams): void;
}
