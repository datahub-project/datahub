import { ApiStatus } from '@datahub/utils/api/shared';
import { OwnerIdType, OwnerSource, OwnerType, OwnerUrnNamespace } from 'datahub-web/utils/api/datasets/owners';

/**
 * Describes the interface for an Owner entity
 */
export interface IOwner {
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
export interface IOwnerResponse {
  owners?: Array<IOwner>;
  fromUpstream: boolean;
  datasetUrn: string;
  // date the ownership information was last modified
  lastModified: number;
  // entity that performed the modification
  actor: string;
}

/**
 * Describes the properties on a response to a request for owner types
 * @interface
 */
export interface IOwnerTypeResponse {
  status: ApiStatus;
  ownerTypes?: Array<OwnerType>;
  msg?: string;
}
