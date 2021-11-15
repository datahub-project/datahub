package com.datahub.authentication;

/**
 * A specific type of Actor on DataHub's platform.
 *
 * Currently the only actor type officially supported, though in the future this may evolve
 * to include SERVICE users.
 */
public enum ActorType {
  /**
   * A user actor, e.g. urn:li:corpuser:johnsmith
   */
  CORP_USER,
}
