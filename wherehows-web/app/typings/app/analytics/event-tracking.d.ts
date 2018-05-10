import { MergedEvent, TrackableEventCategory } from 'wherehows-web/constants/analytics/event-tracking';
import { StringEnumKeyToEnumValue } from 'wherehows-web/typings/generic';
import { ComplianceEvent } from 'wherehows-web/constants/analytics/event-tracking/compliance';

/**
 * Aliases the type for the MergedEvent object
 * @type {TrackableEvent}
 */
type TrackableEvent = typeof MergedEvent;

/**
 * Aliases the type of all events that assigned into the MergeEvent object
 * @type {}
 */
type MergedEventType = typeof ComplianceEvent;

/**
 * Aliases the union interface for all merged events
 */
type MergeUnion = ComplianceEvent & {};

/**
 * Constrains the interface for trackableEvent object, which is a mapping of categories to
 * event enums
 */
type ITrackableEventCategoryEvent = {
  [key in keyof typeof TrackableEventCategory]: StringEnumKeyToEnumValue<keyof TrackableEvent, MergeUnion>
};

/**
 * Describes the interface for Piwik event anatomy
 * @interface IPiwikEvent
 */
interface IPiwikEvent {
  // The category this event should be logged under
  category: TrackableEventCategory;
  // The action that was performed for the event category
  action: TrackableEvent;
  // An optional name for the event
  name?: string;
  // An optional numeric value for the event, e.g. the score or rating of a particular item
  value?: number;
}

export { TrackableEvent, ITrackableEventCategoryEvent, IPiwikEvent, MergedEventType };
