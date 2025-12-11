/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
