package com.datahub.authorization;

import java.util.List;

/**
 * Represents a group of privileges that must <b>ALL</b> be required to authorize a request.
 *
 * <p>That is, an AND of privileges.
 */
public class ConjunctivePrivilegeGroup {
  private final List<String> _requiredPrivileges;

  public ConjunctivePrivilegeGroup(List<String> requiredPrivileges) {
    _requiredPrivileges = requiredPrivileges;
  }

  public List<String> getRequiredPrivileges() {
    return _requiredPrivileges;
  }
}
