import { IOwnershipSource } from '@datahub/metadata-types/types/common/ownership-source';
import { OwnershipType } from '@datahub/metadata-types/constants/common/ownership-type';

/**
 * Ownership information
 * @export
 * @interface IOwner
 */
export interface IOwner {
  // Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name
  owner: string;
  // The type of the ownership
  type: OwnershipType;
  // Source information for the ownership
  source?: IOwnershipSource;
}
