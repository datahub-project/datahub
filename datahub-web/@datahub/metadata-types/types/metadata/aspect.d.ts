import { IChangeAuditStamps } from '@datahub/metadata-types/types/common/change-audit-stamp';
import { Snapshot } from '@datahub/metadata-types/types/metadata/snapshot';
import { ArrayElement } from '@datahub/utils/types/array';

/**
 * A specific metadata aspect for an entity
 * @export
 * @namespace metadata.aspect
 * @interface IBaseAspect
 */
export interface IBaseAspect extends IChangeAuditStamps {
  // The version number of the Metadata
  version: number;
}

/**
 * Aliases a union of all entity aspect types. Both aspects are identical in structure, but once
 * serialized as JSON, there is no real definite way to tell them apart currently since the urn property
 * is serialized as a string
 * @namespace metadata.aspect
 * @type MetadataAspect
 */
export type MetadataAspect = ArrayElement<Snapshot['aspects']>;
