import { IOwner } from '@datahub/metadata-types/types/common/owner';

/**
 * Ownership information of an entity
 * @export
 * @interface IOwnership
 */
export interface IOwnership {
  // List of owners of the entity
  owners: Array<IOwner>;
}
