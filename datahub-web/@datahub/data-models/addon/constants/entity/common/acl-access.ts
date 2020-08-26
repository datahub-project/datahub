/**
 * Enumerates the access control types for an ACL entry
 * @export
 */
export enum AccessControlAccessType {
  Read = 'READ',
  Write = 'WRITE',
  ReadWrite = 'READ_WRITE'
}

/**
 * Possible values for the ACL access status, where expired means we are past the expiration time
 */
export enum AclAccessStatus {
  // Access existed but is past the expiration
  EXPIRED = 'EXPIRED',
  // Access is currently standing
  ACTIVE = 'ACTIVE'
}
