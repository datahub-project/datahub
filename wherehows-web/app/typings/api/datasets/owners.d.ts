import { ApiStatus } from 'wherehows-web/utils/api/shared';
import { OwnerIdType, OwnerSource, OwnerType, OwnerUrnNamespace } from 'wherehows-web/utils/api/datasets/owners';

/**
 * Describes the interface for an Owner entity
 */
interface IOwner {
  confirmedBy: null | string;
  email: null | string;
  idType: OwnerIdType;
  isActive: boolean;
  isGroup: boolean;
  modifiedTime?: number | Date;
  name: string;
  namespace: OwnerUrnNamespace;
  sortId: null | number;
  source: OwnerSource;
  subType: null;
  type: OwnerType;
  userName: string;
}

/**
 * Describes the expected shape of the response for dataset owners endpoint
 */
interface IOwnerResponse {
  status: ApiStatus;
  owners?: Array<IOwner>;
  msg?: string;
}

/**
 * Describes the interface on a response to a POST on the owner endpoint
 */
interface IOwnerPostResponse {
  status: ApiStatus;
  msg?: string;
}

export { IOwnerPostResponse, IOwnerResponse, IOwner };
