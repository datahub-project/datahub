import { IAuditStamp } from '@datahub/metadata-types/types/common/audit-stamp';

/**
 * API format for an institutional memory object
 */
export interface IInstitutionalMemory {
  // url for the link to the provided institutional memory provided by the user
  url: string;
  // user provided description for what the link is
  description: string;
  // metadata model based stamp describing the time and actor that created the institutional memory
  // Will be undefined if it has been recently created by the user
  createStamp?: IAuditStamp;
}

export type InstitutionalMemories = Array<IInstitutionalMemory>;
