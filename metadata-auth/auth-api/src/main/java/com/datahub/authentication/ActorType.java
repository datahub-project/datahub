package com.datahub.authentication;

/**
 * A specific type of Actor on DataHub's platform.
 *
 * <p>Currently the only actor type officially supported, though in the future this may evolve to
 * include service users.
 */
public enum ActorType {
  /** A user actor, e.g. john smith */
  USER,
}
