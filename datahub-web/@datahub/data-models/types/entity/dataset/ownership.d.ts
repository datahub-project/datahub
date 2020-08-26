import { OwnerUrnNamespace } from '@datahub/data-models/constants/entity/dataset/ownership';

/**
 * Describes the interface for a mid-tier Dataset Owner
 */
export interface IOwner {
  // Flag indicating that this owner has been confirmed by another user from the UI,
  // value of this  flag is the confirming user's username
  confirmedBy: null | string;
  // Email address associated with this owner
  email: null | string;
  // Flag indicating that the owner is currently active / inactive within the domain, e.g. organization
  isActive: boolean;
  // Flag indicating that the owner is a group
  isGroup: boolean;
  // Human name for the owner
  name: string;
  // Time when this owner was last modified for this dataset, this is provided as a number by the mid-tier, however
  // locally mutated to be a date due to a legacy implementation
  modifiedTime?: number | Date;
  // Principal type for the owner
  idType: Com.Linkedin.Avro2pegasus.Events.Datavault.PrincipalType;
  // Owner prefix for the urn, indicating the type of owner
  namespace: OwnerUrnNamespace;
  // Provenance for the ownership information
  source: Com.Linkedin.Dataset.OwnershipProvider;
  // The type of ownership the user has in relation to the dataset
  type: Com.Linkedin.Dataset.OwnerCategory; // or? datahub-web/packages/data-portal/app/utils/api/datasets/owners.ts/OwnerType
  // username for the owner
  userName: string;
  // Unused attribute
  sortId: null | number;
  // unused attribute
  subType: null;
}

/**
 * Describes the expected shape of the response from dataset owners endpoint
 * when requesting ownership information
 */
export interface IOwnerResponse {
  // List of dataset owners confirming to the mid-tier IOwner interface
  owners?: Array<IOwner>;
  // Flag indicates if the dataset is from an upstream source
  fromUpstream: boolean;
  // urn for the dataset, however if the fromUpstream flag is truthy, then this will be the upstream dataset's urn
  datasetUrn: string;
  // date the ownership information was last modified
  lastModified: number;
  // entity that performed the modification
  actor: string;
}
