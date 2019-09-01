import { Time } from '@datahub/metadata-types/types/common/time';

/**
 * Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage.
 * @export
 * @namespace common
 * @interface IAuditStamp
 */
export interface IAuditStamp {
  // When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent.
  time: Time;
  // The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change.
  actor: string;
  // The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor.
  impersonator?: string;
}
