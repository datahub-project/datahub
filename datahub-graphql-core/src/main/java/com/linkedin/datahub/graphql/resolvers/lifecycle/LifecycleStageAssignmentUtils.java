package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.ALL_PRIVILEGES_GROUP;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.lifecycle.LifecycleStageTransitionPolicy;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.LifecycleStages;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import javax.annotation.Nonnull;

/** Shared validation for user-initiated lifecycle stage assignments and proposals. */
public final class LifecycleStageAssignmentUtils {

  private LifecycleStageAssignmentUtils() {}

  /**
   * Same rule setLifecycleStage enforces: EDIT_ENTITY (all privileges) or
   * EDIT_ENTITY_LIFECYCLE_STAGE.
   */
  public static boolean canEditLifecycleStage(
      @Nonnull final QueryContext context, @Nonnull final Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.EDIT_ENTITY_LIFECYCLE_STAGE_PRIVILEGE.getType()))));
    return AuthorizationUtils.isAuthorized(
        context, entityUrn.getEntityType(), entityUrn.toString(), orPrivilegeGroups);
  }

  public static void validateUserAssignment(
      @Nonnull QueryContext context,
      @Nonnull EntityService<?> entityService,
      @Nonnull Urn targetUrn,
      @Nonnull Urn stageUrn)
      throws Exception {
    // Read from primary: LifecycleStageTransitionHook judges the write against the committed
    // Status, so a stale replica read here could accept a transition the hook silently reverts.
    final OperationContext primary =
        context.getOperationContext().withReadPreference(ReadPreference.PRIMARY);
    LifecycleStageTypeInfo stageInfo =
        (LifecycleStageTypeInfo)
            entityService.getAspect(primary, stageUrn, LifecycleStageTypeMapper.INFO_ASPECT, 0L);
    if (stageInfo == null) {
      throw invalidStage(String.format("Lifecycle stage '%s' does not exist.", stageUrn));
    }

    if (!LifecycleStages.appliesToEntityType(stageInfo, targetUrn.getEntityType())) {
      throw invalidStage(
          String.format(
              "Lifecycle stage '%s' does not apply to entity type '%s'.",
              stageUrn, targetUrn.getEntityType()));
    }

    if (!isUserAssignableForEntity(stageUrn, stageInfo, targetUrn.getEntityType())) {
      throw invalidStage(
          isDocumentDraft(stageUrn, targetUrn.getEntityType())
              ? "The DRAFT lifecycle stage is reserved for internal document working copies."
              : String.format("Lifecycle stage '%s' is system-managed.", stageUrn));
    }

    final Status currentStatus =
        (Status) entityService.getAspect(primary, targetUrn, Constants.STATUS_ASPECT_NAME, 0L);
    final Urn currentStage =
        currentStatus != null && currentStatus.hasLifecycleStage()
            ? currentStatus.getLifecycleStage()
            : null;
    final LifecycleStageTransitionPolicy policy =
        stageInfo.hasTransitionPolicy() ? stageInfo.getTransitionPolicy() : null;
    if (!LifecycleStages.isTransitionAllowed(currentStage, stageUrn, policy)) {
      throw new DataHubGraphQLException(
          String.format(
              "Moving from %s to lifecycle stage '%s' is not allowed. Allowed previous stages: %s.",
              currentStage == null ? "no stage" : "'" + currentStage + "'",
              stageUrn,
              policy.getAllowedPreviousStages()),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
  }

  public static boolean isUserAssignableForEntity(
      @Nonnull Urn stageUrn,
      @Nonnull LifecycleStageTypeInfo stageInfo,
      @Nonnull String entityType) {
    return LifecycleStages.appliesToEntityType(stageInfo, entityType)
        && stageInfo.getSettings().isUserAssignable()
        && !isDocumentDraft(stageUrn, entityType);
  }

  private static boolean isDocumentDraft(Urn stageUrn, String entityType) {
    // DRAFT predates userAssignable and was bootstrapped as generally assignable. Keep this
    // document-specific guard until existing deployments have migrated that stage setting.
    return Constants.DOCUMENT_ENTITY_NAME.equals(entityType)
        && Constants.DRAFT_LIFECYCLE_STAGE_URN.equals(stageUrn.toString());
  }

  private static DataHubGraphQLException invalidStage(String reason) {
    return new DataHubGraphQLException(
        reason
            + " Call listLifecycleStages and choose an applicable stage with userAssignable=true.",
        DataHubGraphQLErrorCode.BAD_REQUEST);
  }
}
