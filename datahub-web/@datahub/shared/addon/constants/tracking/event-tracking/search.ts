import { IBaseTrackingEvent } from '@datahub/shared/types/tracking/event-tracking';
import { TrackingEventCategory } from '@datahub/shared/constants/tracking/event-tracking';
/**
 * Enum of search action clicks meant to be used for search tracking event keys
 * @enum
 */
enum searchActions {
  SearchResultClick = 'SearchResultClick',
  SearchResultSATClick = 'SearchResultSATClick',
  SearchResultFacetClick = 'SearchResultFacetClick'
}

export const searchTrackingEvent: Record<searchActions, IBaseTrackingEvent> = {
  SearchResultClick: {
    category: TrackingEventCategory.Search,
    action: 'SearchResultClickEvent'
  },
  SearchResultSATClick: {
    category: TrackingEventCategory.Search,
    action: 'SearchResultSATClickEvent'
  },
  SearchResultFacetClick: {
    category: TrackingEventCategory.Search,
    action: 'SearchResultFacetClickEvent'
  }
};
