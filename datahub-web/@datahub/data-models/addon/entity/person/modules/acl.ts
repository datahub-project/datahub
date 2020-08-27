import { AccessControlAccessType, AclAccessStatus } from '@datahub/data-models/constants/entity/common/acl-access';

/**
 * The AclAccess class represents a wrapper around an entity for the sake of representing an acl
 * access object to that specific entity. This provides the metadata associated over that
 */
export class AclAccess<T> {
  /**
   * Reference to the underlying entity for which this acl access object represents
   */
  entity: T;

  /**
   * The environment for which this acl access object is valid. Internally this equates to "fabric"
   * but may also be expanded to include various platforms
   */
  environment: string;

  /**
   * The type of access that this access object is for, i.e. READ, WRITE
   */
  accessType: Array<AccessControlAccessType>;

  /**
   * The current status of the access, i.e. ACTIVE or EXPIRED
   */
  status: AclAccessStatus;

  /**
   * If the ACL access can expire, then we will store it here. Otherwise, this will be null and
   * implies that the user has standing access to the underlying entity data
   */
  expiration: number | null;

  /**
   * The business justification used previously to gain acl access for this entity
   * Optional - only defined if owner is accessing own JIT ACLs
   */
  businessJustification?: string;

  constructor(
    entity: T,
    metadata: {
      environment: string;
      accessType: Array<AccessControlAccessType>;
      status: AclAccessStatus;
      expiration: number | null;
      businessJustification?: string;
    }
  ) {
    const { environment, accessType, status, expiration = null, businessJustification } = metadata;
    this.entity = entity;

    this.environment = environment;
    this.accessType = accessType;
    this.status = status;
    this.expiration = expiration;
    this.businessJustification = businessJustification;
  }
}
