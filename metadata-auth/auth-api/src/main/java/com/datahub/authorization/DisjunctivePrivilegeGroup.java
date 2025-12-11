/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authorization;

import java.util.Collection;

/**
 * Represents a group of privilege groups, any of which must be authorized to authorize a request.
 *
 * <p>That is, an OR of privilege groups.
 */
public class DisjunctivePrivilegeGroup {
  private final Collection<ConjunctivePrivilegeGroup> _authorizedPrivilegeGroups;

  public DisjunctivePrivilegeGroup(
      Collection<ConjunctivePrivilegeGroup> authorizedPrivilegeGroups) {
    _authorizedPrivilegeGroups = authorizedPrivilegeGroups;
  }

  public Collection<ConjunctivePrivilegeGroup> getAuthorizedPrivilegeGroups() {
    return _authorizedPrivilegeGroups;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DisjunctivePrivilegeGroup that = (DisjunctivePrivilegeGroup) o;

    return _authorizedPrivilegeGroups.equals(that._authorizedPrivilegeGroups);
  }

  @Override
  public int hashCode() {
    return _authorizedPrivilegeGroups.hashCode();
  }

  @Override
  public String toString() {
    return "DisjunctivePrivilegeGroup{"
        + "_authorizedPrivilegeGroups="
        + _authorizedPrivilegeGroups
        + '}';
  }
}
