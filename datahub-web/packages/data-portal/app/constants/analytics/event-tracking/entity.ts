import { IBaseTrackingEvent, TrackingEvents } from 'wherehows-web/typings/app/analytics/event-tracking';
import { arrayReduce } from 'wherehows-web/utils/array';
import { TrackingEventCategory } from 'wherehows-web/constants/analytics/event-tracking/index';
import { DataModelName } from '@datahub/data-models/constants/entity';

interface IEntityTrackingEvent extends IBaseTrackingEvent {
  // Required name of the entity
  name: string;
}

/**
 * Initial map of event names to entity tracking event instances
 * @type Record<string, Partial<IEntityTrackingEvent>>
 */
const entityTrackingEvent: Record<string, Partial<IEntityTrackingEvent>> = {
  ViewEvent: {
    category: TrackingEventCategory.Entity,
    action: 'EntityViewEvent'
  }
};

const entityTrackingEventAccumulator: Partial<typeof entityTrackingEvent> = {};

/**
 * Augments a base tracking event instance with a required name attribute
 * @param {Entity} name the name of the entity to track
 */
const addTrackingEventEntityNameFor = (name: DataModelName) => (
  events: TrackingEvents,
  [eventName, trackingEvent]: [string, Partial<IEntityTrackingEvent>]
) => ({ ...events, [eventName]: { ...trackingEvent, name } });

/**
 * For each event in entityTrackingEvent insert an entity name attribute
 * @param {Entity} entityName
 */
export const createEntityTrackingEventsFor = (entityName: DataModelName) =>
  arrayReduce(addTrackingEventEntityNameFor(entityName), entityTrackingEventAccumulator)(
    Object.entries([entityTrackingEvent])
  );
