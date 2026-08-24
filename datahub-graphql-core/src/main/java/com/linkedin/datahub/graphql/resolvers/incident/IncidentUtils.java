package com.linkedin.datahub.graphql.resolvers.incident;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.IncidentPriority;
import com.linkedin.incident.IncidentAssignee;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentStage;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class IncidentUtils {

  public static List<Urn> stringsToUrns(List<String> urns) {
    return urns.stream()
        .map(
            rawUrn -> {
              try {
                return Urn.createFromString(rawUrn);
              } catch (Exception e) {
                return null;
              }
            })
        .filter(Objects::nonNull)
        .distinct()
        .toList();
  }

  /**
   * Resolves the URN that an incident edit is authorized against.
   *
   * <p>For a schemaField this is the parent entity encoded in the field URN, not the field itself.
   * There is no schemaField resource type in {@link PoliciesConfig}, so a dataset-scoped policy
   * never matches a field URN and only platform admins would pass the check. Authorizing on the
   * parent matches how the field page already reads its permissions, and it matches column edits,
   * where EDIT_DATASET_COL_* is a dataset privilege rather than a field-entity one.
   *
   * <p>The parent is usually a dataset, but GraphQL allows dashboard and chart parents too, so the
   * parent is read from the URN rather than assumed. A field URN that does not parse falls back to
   * itself, which preserves the previous behaviour instead of failing open.
   *
   * <p>This applies to the incident edit check only. View restrictions and EDIT_DATASET_COL_* are
   * unaffected.
   */
  @Nonnull
  public static Urn getIncidentAuthorizationUrn(@Nonnull final Urn resourceUrn) {
    if (!Constants.SCHEMA_FIELD_ENTITY_NAME.equals(resourceUrn.getEntityType())) {
      return resourceUrn;
    }
    return SchemaFieldUtils.parseSchemaFieldUrn(resourceUrn)
        .map(Pair::getFirst)
        .orElse(resourceUrn);
  }

  public static boolean isAuthorizedToEditIncidentForResource(
      final Urn resourceUrn, final QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                AuthorizationUtils.ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_INCIDENTS_PRIVILEGE.getType()))));

    final Urn authorizationUrn = getIncidentAuthorizationUrn(resourceUrn);
    return AuthorizationUtils.isAuthorized(
        context, authorizationUrn.getEntityType(), authorizationUrn.toString(), orPrivilegeGroups);
  }

  @Nullable
  public static Integer mapIncidentPriority(@Nullable final IncidentPriority priority) {
    if (priority == null) {
      return null;
    }
    switch (priority) {
      case LOW:
        return 3;
      case MEDIUM:
        return 2;
      case HIGH:
        return 1;
      case CRITICAL:
        return 0;
      default:
        throw new IllegalArgumentException("Invalid incident priority: " + priority);
    }
  }

  @Nullable
  public static IncidentAssigneeArray mapIncidentAssignees(
      @Nullable final List<String> assignees, @Nonnull final AuditStamp auditStamp) {
    if (assignees == null) {
      return null;
    }
    return new IncidentAssigneeArray(
        assignees.stream()
            .map(assignee -> createAssignee(assignee, auditStamp))
            .collect(Collectors.toList()));
  }

  @Nonnull
  public static IncidentStatus mapIncidentStatus(
      @Nullable final com.linkedin.datahub.graphql.generated.IncidentStatusInput input,
      @Nonnull final AuditStamp auditStamp) {
    if (input == null) {
      return new IncidentStatus().setState(IncidentState.ACTIVE).setLastUpdated(auditStamp);
    }

    IncidentStatus status = new IncidentStatus();
    status.setState(IncidentState.valueOf(input.getState().toString()));
    status.setStage(
        input.getStage() == null ? null : IncidentStage.valueOf(input.getStage().toString()),
        SetMode.REMOVE_IF_NULL);
    if (input.getMessage() != null) {
      status.setMessage(input.getMessage());
    }
    status.setLastUpdated(auditStamp);
    return status;
  }

  private static IncidentAssignee createAssignee(
      @Nonnull final String assigneeUrn, @Nonnull final AuditStamp auditStamp) {
    return new IncidentAssignee().setActor(UrnUtils.getUrn(assigneeUrn)).setAssignedAt(auditStamp);
  }

  private IncidentUtils() {}
}
