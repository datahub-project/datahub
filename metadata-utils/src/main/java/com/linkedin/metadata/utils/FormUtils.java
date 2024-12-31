package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.form.FormActorAssignment;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class FormUtils {

  private FormUtils() {}

  public static boolean isFormAssignedToUser(
      @Nonnull final FormActorAssignment parent,
      @Nonnull final Urn userUrn,
      @Nonnull final List<Urn> groupUrns) {
    // Assigned urn and group urns
    final Set<String> assignedUserUrns =
        parent.getUsers() != null
            ? parent.getUsers().stream().map(Urn::toString).collect(Collectors.toSet())
            : Collections.emptySet();

    final Set<String> assignedGroupUrns =
        parent.getGroups() != null
            ? parent.getGroups().stream().map(Urn::toString).collect(Collectors.toSet())
            : Collections.emptySet();

    // First check whether user is directly assigned.
    if (assignedUserUrns.size() > 0) {
      boolean isUserAssigned = assignedUserUrns.contains(userUrn.toString());
      if (isUserAssigned) {
        return true;
      }
    }

    // Next check whether the user is assigned indirectly, by group.
    if (assignedGroupUrns.size() > 0) {
      boolean isUserGroupAssigned =
          groupUrns.stream().anyMatch(groupUrn -> assignedGroupUrns.contains(groupUrn.toString()));
      if (isUserGroupAssigned) {
        return true;
      }
    }

    return false;
  }
}
