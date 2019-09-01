import { OwnershipSourceType } from '@datahub/metadata-types/constants/common/ownership-source-type';

/**
 * Source/provider of the ownership information
 * @export
 * @interface IOwnershipSource
 */
export interface IOwnershipSource {
  // The type of the source
  type: OwnershipSourceType;
  // A reference URL for the source
  url?: string;
}
