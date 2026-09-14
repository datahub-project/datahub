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
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.IncidentPriority;
import com.linkedin.datahub.graphql.generated.UpdateIncidentInput;
import com.linkedin.datahub.graphql.generated.UpsertIncidentInput;
import com.linkedin.incident.IncidentAssignee;
import com.linkedin.incident.IncidentAssigneeArray;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentStage;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.IncidentInfoPatch;
import com.linkedin.metadata.service.IncidentInfoUpsert;
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

  /** Maps the GraphQL PATCH input into the service's GraphQL-independent patch object. */
  @Nonnull
  public static IncidentInfoPatch mapIncidentUpdate(
      @Nonnull final UpdateIncidentInput input, @Nonnull final AuditStamp auditStamp) {
    IncidentInfoPatch.Builder builder =
        IncidentInfoPatch.builder()
            .title(input.getTitle())
            .description(input.getDescription())
            .startedAt(input.getStartedAt())
            .priority(mapIncidentPriority(input.getPriority()))
            .entities(
                input.getResourceUrns() == null ? null : stringsToUrns(input.getResourceUrns()))
            .assignees(mapIncidentAssignees(input.getAssigneeUrns(), auditStamp));
    if (input.getStatus() != null) {
      builder.status(mapIncidentStatus(input.getStatus(), auditStamp));
    }
    return builder.build();
  }

  /** Maps the complete editor input into the service's GraphQL-independent upsert object. */
  @Nonnull
  public static IncidentInfoUpsert mapIncidentUpsert(
      @Nonnull final UpsertIncidentInput input, @Nonnull final AuditStamp auditStamp) {
    IncidentInfoUpsert.Builder builder =
        IncidentInfoUpsert.builder()
            .title(input.getTitle())
            .description(input.getDescription())
            .priority(mapIncidentPriority(input.getPriority()))
            .entities(stringsToUrns(input.getResourceUrns()))
            .assignees(mapIncidentAssignees(input.getAssigneeUrns(), auditStamp));
    builder.status(mapIncidentStatus(input.getStatus(), auditStamp));
    return builder.build();
  }

  /** Verifies permissions for an incident and any newly linked resources. */
  public static void verifyAuthorizationOrThrow(
      @Nonnull QueryContext context,
      @Nonnull IncidentInfo info,
      @Nullable List<String> newResourceUrns)
      throws AuthorizationException {
    final List<Urn> existingResourceUrns = info.getEntities();
    for (Urn resourceUrn : existingResourceUrns) {
      if (!isAuthorizedToEditIncidentForResource(resourceUrn, context)) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }
    }
    if (newResourceUrns != null) {
      List<Urn> parsedResourceUrns = stringsToUrns(newResourceUrns);
      if (parsedResourceUrns.isEmpty()) {
        throw new IllegalArgumentException("resourceUrns cannot be empty if provided");
      }
      for (Urn resourceUrn : parsedResourceUrns) {
        if (!existingResourceUrns.contains(resourceUrn)
            && !isAuthorizedToEditIncidentForResource(resourceUrn, context)) {
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        }
      }
    }
  }

  private static IncidentAssignee createAssignee(
      @Nonnull final String assigneeUrn, @Nonnull final AuditStamp auditStamp) {
    return new IncidentAssignee().setActor(UrnUtils.getUrn(assigneeUrn)).setAssignedAt(auditStamp);
  }

  private IncidentUtils() {}
}
