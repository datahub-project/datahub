import { IResolvedOwner } from 'datahub-web/typings/api/ownership/resolved-owner';

export interface IResolvedOwnership {
  // Dataset this ownership metadata is associated with.
  dataset: string;
  // An AuditStamp corresponding to the last modification of the original ownership.
  lastModified: string;
  // List of resolved confirmed owners
  owners: Array<IResolvedOwner>;
}
