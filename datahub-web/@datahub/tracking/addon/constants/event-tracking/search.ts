import { IBaseTrackingEvent } from '@datahub/tracking/types/event-tracking';
import { TrackingEventCategory } from '@datahub/tracking/constants/event-tracking';
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
