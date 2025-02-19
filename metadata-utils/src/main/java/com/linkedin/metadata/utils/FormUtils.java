package com.linkedin.metadata.utils;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class FormUtils {

  private static final String COMPLETED_FORMS = "completedForms";
  private static final String INCOMPLETE_FORMS = "incompleteForms";

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

  public static Criterion getFormOwnershipCriterion(
      @Nonnull final Urn userUrn, @Nonnull final List<Urn> groupUrns) {
    StringArray ownershipUrns = new StringArray();
    ownershipUrns.add(userUrn.toString());
    groupUrns.forEach(groupUrn -> ownershipUrns.add(groupUrn.toString()));

    // create filter for entities owned by this user or one of their groups
    return CriterionUtils.buildCriterion("owners", Condition.EQUAL, ownershipUrns);
  }

  // Filter for assets where a given form is not on the asset yet
  public static Filter buildAssetsMissingFormFilter(@Nonnull final String formUrn) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                ImmutableList.of(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(
                                    CriterionUtils.buildCriterion(
                                        INCOMPLETE_FORMS, Condition.EQUAL, true, formUrn),
                                    CriterionUtils.buildCriterion(
                                        COMPLETED_FORMS, Condition.EQUAL, true, formUrn)))))));
  }
}
