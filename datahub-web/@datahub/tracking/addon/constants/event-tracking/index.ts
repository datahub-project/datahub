import { TrackingEvents, IBaseTrackingEvent } from '@datahub/tracking/types/event-tracking';

/**
 * String values for categories that can be tracked with the application
 * @export
 * @enum {string}
 */
export enum TrackingEventCategory {
  DatasetCompliance = 'DATASET_COMPLIANCE',
  Search = 'SEARCH',
  Entity = 'ENTITY'
}

/**
 * Enumerates the available tracking goals
 * @export
 * @enum {number}
 */
export enum TrackingGoal {
  // Satisfied Search Clicks goal tracking
  SatClick = 1
}

// Convenience alias for insertTrackingEventsCategoryFor return type
type InsertTrackingEventsCategoryForReturn = Record<string, IBaseTrackingEvent | Partial<IBaseTrackingEvent>>;

/**
 * Augments a tracking event partial with category information
 * @param {TrackingEvents} events the events mapping
 * @param {[string, Partial<IBaseTrackingEvent>]} [eventName, trackingEvent]
 */
export const insertTrackingEventsCategoryFor = (
  category: TrackingEventCategory
): ((
  events: TrackingEvents,
  eventNameAndEvent: [string, Partial<IBaseTrackingEvent>]
) => InsertTrackingEventsCategoryForReturn) => (
  events: TrackingEvents,
  [eventName, trackingEvent]: [string, Partial<IBaseTrackingEvent>]
): InsertTrackingEventsCategoryForReturn => ({
  ...events,
  [eventName]: { ...trackingEvent, category }
});
