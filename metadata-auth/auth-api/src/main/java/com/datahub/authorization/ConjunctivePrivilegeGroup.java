package com.datahub.authorization;

import java.util.Collection;

/**
 * Represents a group of privileges that must <b>ALL</b> be required to authorize a request.
 *
 * <p>That is, an AND of privileges.
 */
public class ConjunctivePrivilegeGroup {
  private final Collection<String> _requiredPrivileges;

  public ConjunctivePrivilegeGroup(Collection<String> requiredPrivileges) {
    _requiredPrivileges = requiredPrivileges;
  }

  public Collection<String> getRequiredPrivileges() {
    return _requiredPrivileges;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ConjunctivePrivilegeGroup that = (ConjunctivePrivilegeGroup) o;

    return _requiredPrivileges.equals(that._requiredPrivileges);
  }

  @Override
  public int hashCode() {
    return _requiredPrivileges.hashCode();
  }

  @Override
  public String toString() {
    return "ConjunctivePrivilegeGroup{" + "_requiredPrivileges=" + _requiredPrivileges + '}';
  }
}
