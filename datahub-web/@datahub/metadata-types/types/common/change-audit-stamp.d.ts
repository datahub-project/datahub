import { IAuditStamp } from '@datahub/metadata-types/types/common/audit-stamp';

/**
 * Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage.
 * @export
 * @namespace common
 * @interface IChangeAuditStamps
 */
export interface IChangeAuditStamps {
  // An AuditStamp corresponding to the creation of this resource/association/sub-resource
  created: IAuditStamp;
  // An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created
  lastModified: IAuditStamp;
  // An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics.
  deleted?: IAuditStamp;
}
