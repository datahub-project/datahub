/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.timeline.data;

public enum ChangeOperation {
  /** Something is added to an entity, e.g. tag, glossary term. */
  ADD,
  /** An entity is modified. e.g. Domain, description is updated. */
  MODIFY,
  /** Something is removed from an entity. e.g. tag, glossary term. */
  REMOVE,
  /** Entity is created. */
  CREATE,
  /** Entity is hard-deleted. */
  HARD_DELETE,
  /** Entity is soft-deleted. */
  SOFT_DELETE,
  /** Entity is reinstated after being soft-deleted. */
  REINSTATE,
  /** Run has STARTED */
  STARTED,
  /** Run is completed */
  COMPLETED
}
