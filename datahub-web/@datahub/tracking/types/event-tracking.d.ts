import { TrackingEventCategory, TrackingGoal } from '@datahub/tracking/constants/event-tracking';

/**
 * Describes the interface for a tracking event.
 * More specific traffic events may extend from this.
 * @interface IBaseTrackingEvent
 */
export interface IBaseTrackingEvent {
  // The category this event should be logged under e.g. Search
  category: TrackingEventCategory;
  // The action that was performed for the event category e.g. SearchResultClickEvent
  action: string;
  // An optional name e.g. the urn of the search item
  name?: string;
  // An optional value for the event, e.g. the score or rating of a particular item
  value?: number | string;
}

/**
 * Describes the interface for a tracking goal
 * @export
 * @interface IBaseTrackingGoal
 */
export interface IBaseTrackingGoal {
  // The name of the goal being tracked, usually a numeric identifier
  name: TrackingGoal;
}

/**
 * A mapping of events names to instances of tracking events
 * @template K the event names to key by
 * @template T types that are assignable to the base tracking event
 * @alias {Record<Extract<K, string>,T>}
 */
export type TrackingEvents<K = string, T extends IBaseTrackingEvent = IBaseTrackingEvent> = Record<
  Extract<K, string>,
  T
>;
