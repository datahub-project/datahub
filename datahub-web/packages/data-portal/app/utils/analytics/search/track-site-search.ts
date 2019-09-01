import {
  IPiwikTrackSiteSearchParams,
  ITrackPiwikSiteSearchReturnFn
} from 'wherehows-web/typings/app/analytics/site-search-tracking/piwik';
import { TrackSearchAdapter } from 'wherehows-web/constants/analytics/site-search-tracking/adapters';

/**
 * Manually track Piwik siteSearch using the `trackSiteSearch` api
 *  rather than using Piwik's default reading of url's containing the
 *  "search", "q", "query", "s", "searchword", "k" and "keyword", keywords
 * @link https://developer.piwik.org/guides/tracking-javascript-guide#internal-search-tracking
 * @param {Window['_paq']} piwikActivityQueue
 * @returns {((arg: IPiwikTrackSiteSearchParams) => void)}
 */
const trackPiwikSiteSearch = (piwikActivityQueue: Window['_paq']): ITrackPiwikSiteSearchReturnFn => ({
  keyword,
  entity,
  searchCount
}: IPiwikTrackSiteSearchParams): void =>
  void piwikActivityQueue.push(['trackSiteSearch', keyword, entity, searchCount]);

/**
 * Describes the index signature for the strategy object used in selecting the tracking method for search.
 * Each available approach is keyed by TrackSearchAdapter
   * @type {
    [TrackSearchAdapter.Piwik]: (piwikActivityQueue: any[]) => ITrackPiwikSiteSearchReturnFn;
}
   */
export const adapterStrategy = {
  [TrackSearchAdapter.Piwik]: trackPiwikSiteSearch
};

/**
 * Wrapper function to track site search activity based on the selected TrackSearchAdapter
 * @template T
 * @param {T} adapter
 * @returns {(typeof adapterStrategy)[T]}
 */
export const trackSiteSearch = <T extends TrackSearchAdapter>(adapter: T): (typeof adapterStrategy)[T] =>
  adapterStrategy[adapter];
