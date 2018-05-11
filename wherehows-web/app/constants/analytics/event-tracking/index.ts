import { ITrackableEventCategoryEvent, MergedEventType } from 'wherehows-web/typings/app/analytics/event-tracking';
import { ComplianceEvent } from 'wherehows-web/constants/analytics/event-tracking/compliance';
import { arrayMap, arrayReduce, isListUnique } from 'wherehows-web/utils/array';
import { assert } from '@ember/debug';

/**
 * Enumerates the available event categories that can be or are being tracked
 * @enum {string}
 */
enum TrackableEventCategory {
  Compliance = 'compliance'
}

/**
 * Maps the event categories to its related events enum i.e. category -> events
 * @type {ITrackableEventCategoryEvent}
 */
const trackableEvent: ITrackableEventCategoryEvent = {
  Compliance: ComplianceEvent
};

/**
 * Merges all trackable events into a combined map
 * as more events are added, they can be merged into this map
 * @type {{} & ComplianceEvent}
 */
const MergedEvent = ((events: Array<object>) => {
  const eventNames: Array<string> = [].concat.apply([], arrayMap(Object.keys)(events));
  assert('Events actions must be unique across categories', isListUnique(eventNames));

  return arrayReduce((mergedEvents, event) => ({ ...mergedEvents, ...event }), <MergedEventType>{})(events);
})([ComplianceEvent]);

export * from 'wherehows-web/constants/analytics/event-tracking/compliance';
export { MergedEvent, TrackableEventCategory, trackableEvent };
