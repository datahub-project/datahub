import { ApiStatus } from 'wherehows-web/utils/api/shared';
import { OwnerType } from 'wherehows-web/utils/api/datasets/owners';

/**
 * Accepted  string values for the Owner type
 */
type OwnerTypeLiteral = OwnerType.User | OwnerType.Group;

/**
 * Accepted string values for the namespace of a user
 */
type OwnerUrnLiteral = 'urn:li:corpuser' | 'urn:li:corpGroup';

/**
 * Describes the interface for an Owner entity
 */
export interface IOwner {
  confirmedBy: null | string;
  email: string;
  idType: OwnerTypeLiteral;
  isActive: boolean;
  isGroup: boolean;
  modifiedTime: number | Date;
  name: string;
  namespace: OwnerUrnLiteral;
  sortId: null | number;
  source: string;
  subType: null;
  type: string;
  userName: string;
}

/**
 * Describes the expected shape of the response for dataset owners endpoint
 */
export interface IOwnerResponse {
  status: ApiStatus;
  owners?: Array<IOwner>;
}
