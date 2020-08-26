import { IBaseTrackingEvent, TrackingEvents } from '@datahub/shared/types/tracking/event-tracking';
import {
  insertTrackingEventsCategoryFor,
  TrackingEventCategory
} from '@datahub/shared/constants/tracking/event-tracking/index';

/**
 * TODO: META-8050 transition deprecated event references
 * Enumerates the available compliance metadata events
 * @deprecated Replaced with complianceTrackingEvents
 * @link complianceTrackingEvents
 */
export enum ComplianceEvent {
  Cancel = 'CancelEditComplianceMetadata',
  Next = 'NextComplianceMetadataStep',
  ManualApply = 'AdvancedEditComplianceMetadataStep',
  Previous = 'PreviousComplianceMetadataStep',
  Edit = 'BeginEditComplianceMetadata',
  Download = 'DownloadComplianceMetadata',
  Upload = 'UploadComplianceMetadata',
  SetUnspecifiedAsNone = 'SetUnspecifiedFieldsAsNone',
  FieldIdentifier = 'ComplianceMetadataFieldIdentifierSelected',
  FieldFormat = 'ComplianceMetadataFieldFormatSelected',
  Save = 'SaveComplianceMetadata'
}

/**
 * Initial map if event names to partial base tracking event with actions
 * @type {Record<string, Partial<IBaseTrackingEvent>>}
 */
const complianceTrackingEvent: Record<string, Partial<IBaseTrackingEvent>> = {
  CancelEvent: {
    action: 'CancelEditComplianceMetadataEvent'
  },
  NextStepEvent: {
    action: 'NextComplianceMetadataStepEvent'
  },
  ManualApplyEvent: {
    action: 'AdvancedEditComplianceMetadataStepEvent'
  },
  PreviousStepEvent: {
    action: 'PreviousComplianceMetadataStepEvent'
  },
  EditEvent: {
    action: 'BeginEditComplianceMetadataEvent'
  },
  DownloadEvent: {
    action: 'DownloadComplianceMetadataEvent'
  },
  UploadEvent: {
    action: 'UploadComplianceMetadataEvent'
  },
  SetUnspecifiedAsNoneEvent: {
    action: 'SetUnspecifiedFieldsAsNoneEvent'
  },
  FieldIdentifierEvent: {
    action: 'ComplianceMetadataFieldIdentifierSelectedEvent'
  },
  FieldFormatEvent: {
    action: 'ComplianceMetadataFieldFormatSelectedEvent'
  },
  SaveEvent: {
    action: 'SaveComplianceMetadataEvent'
  }
};

/**
 * The accumulator object to build attributes for a tracking event
 * @type {Partial<Record<string, Partial<IBaseTrackingEvent>>>}
 */
const complianceTrackingEventsAccumulator: Partial<typeof complianceTrackingEvent> = {};

/**
 * Compliance tracking events with required base tracking event attributes
 * @type {TrackingEvents<keyof typeof complianceTrackingEvent, IBaseTrackingEvent>}
 */
export const complianceTrackingEvents = Object.entries(complianceTrackingEvent).reduce(
  insertTrackingEventsCategoryFor(TrackingEventCategory.DatasetCompliance),
  complianceTrackingEventsAccumulator
) as TrackingEvents<keyof typeof complianceTrackingEvent>;
