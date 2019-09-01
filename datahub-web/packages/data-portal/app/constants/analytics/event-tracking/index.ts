import { TrackingEvents, IBaseTrackingEvent } from 'wherehows-web/typings/app/analytics/event-tracking';

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

/**
 * Augments a tracking event partial with category information
 * @param {TrackingEvents} events the events mapping
 * @param {[string, Partial<IBaseTrackingEvent>]} [eventName, trackingEvent]
 */
export const insertTrackingEventsCategoryFor = (category: TrackingEventCategory) => (
  events: TrackingEvents,
  [eventName, trackingEvent]: [string, Partial<IBaseTrackingEvent>]
) => ({ ...events, [eventName]: { ...trackingEvent, category } });
