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
}
