import { IBaseTrackingEvent } from 'wherehows-web/typings/app/analytics/event-tracking';
import { TrackingEventCategory } from 'wherehows-web/constants/analytics/event-tracking';
/**
 * Tag string literal union for search tracking event keys
 * @alias {string}
 */
type searchActions = 'SearchResultClick' | 'SearchResultSATClick';

export const searchTrackingEvent: Record<searchActions, IBaseTrackingEvent> = {
  SearchResultClick: {
    category: TrackingEventCategory.Search,
    action: 'SearchResultClickEvent'
  },
  SearchResultSATClick: {
    category: TrackingEventCategory.Search,
    action: 'SearchResultSATClickEvent'
  }
};
