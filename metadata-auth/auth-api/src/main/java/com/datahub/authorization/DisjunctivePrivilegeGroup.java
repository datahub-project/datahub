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
