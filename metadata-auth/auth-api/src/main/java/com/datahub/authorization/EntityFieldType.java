package com.datahub.authorization;

/**
 * List of entity field types to fetch for a given entity
 */
public enum EntityFieldType {
  /**
   * Type of the entity (e.g. dataset, chart)
   */
  TYPE,
  /**
   * Urn of the entity
   */
  URN,
  /**
   * Owners of the entity
   */
  OWNER,
  /**
   * Domains of the entity
   */
  DOMAIN,
  /**
   * Groups of which the entity (only applies to corpUser) is a member
   */
  GROUP_MEMBERSHIP
}
