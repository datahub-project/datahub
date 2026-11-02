package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SUB_TYPES_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.util.RecordUtils;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Document-specific authorization: privilege relationships for CRUD plus bridge-aware VIEW.
 *
 * <p>{@link #canViewDocumentEntity} and related helpers are the shared privilege evaluator
 * (flag-independent). API activation for OpenAPI/Rest.li is handled by {@link
 * #isAPIAuthorizedDocumentUrns}, which is active only when REST API authorization is enabled.
 * GraphQL View Authorization activation is handled by GraphQL wrappers such as {@code
 * AuthorizationUtils.canViewDocument}.
 *
 * <p>Generic API callers should use {@code EntityAuthorizationUtils}, which delegates here only for
 * document URNs. View decision:
 *
 * <ol>
 *   <li>Platform {@code MANAGE_DOCUMENTS} → allow
 *   <li>ENTITY READ on the document URN → allow
 *   <li>Confirmed bridge → ENTITY READ on source entity (unresolvable source → deny)
 * </ol>
 */
@Slf4j
public final class DocumentAuthorizationUtils {

  public static final String BRIDGE_TYPE_PROPERTY = "bridge_type";
  public static final String BRIDGE_SOURCE_ENTITY_PROPERTY = "bridge_source_entity";
  public static final String BRIDGE_DOC_ID_PREFIX = "bridge-";
  public static final String BRIDGE_DOCUMENT_SUBTYPE = "Bridge Document";
  public static final String BRIDGE_SOURCE_RESOLUTION_FAILED_METRIC =
      "bridgeSourceUrnResolutionFailed";

  private DocumentAuthorizationUtils() {}

  /**
   * Authorizes document URNs for API surfaces. READ uses bridge-aware VIEW when REST API
   * authorization is enabled. CREATE/UPDATE distinguish existing entities from upsert-created
   * entities so CREATE privileges cannot overwrite existing documents and UPDATE privileges alone
   * cannot create missing ones.
   *
   * <p>Callers with mixed entity types should use {@code EntityAuthorizationUtils} instead.
   */
  public static boolean isAPIAuthorizedDocumentUrns(
      @Nonnull OperationContext opContext,
      @Nonnull ApiOperation apiOperation,
      @Nonnull Collection<Urn> documentUrns) {
    if (documentUrns.isEmpty()) {
      return true;
    }
    if (!AuthUtil.isRestApiAuthorizationEnabled()) {
      return true;
    }
    if (apiOperation == UPDATE || apiOperation == CREATE) {
      Map<Urn, Boolean> existence =
          opContext.getAspectRetriever().entityExists(opContext, Set.copyOf(documentUrns));
      List<Urn> existingDocuments =
          documentUrns.stream().filter(urn -> existence.getOrDefault(urn, false)).toList();
      List<Urn> newDocuments =
          documentUrns.stream().filter(urn -> !existence.getOrDefault(urn, false)).toList();
      return (existingDocuments.isEmpty()
              || AuthUtil.isAPIAuthorizedEntityUrns(opContext, UPDATE, existingDocuments))
          && (newDocuments.isEmpty()
              || AuthUtil.isAPIAuthorizedEntityUrns(opContext, CREATE, newDocuments));
    }
    if (apiOperation != READ) {
      return AuthUtil.isAPIAuthorizedEntityUrns(opContext, apiOperation, documentUrns);
    }
    return canViewDocumentEntities(opContext, opContext.getAspectRetriever(), documentUrns);
  }

  /**
   * Returns the effective (ChangeType, Urn) used to authorize a document ingest proposal.
   * Update-like writes on missing documents become {@link ChangeType#CREATE_ENTITY}; non-document
   * URNs and other change types are returned unchanged.
   */
  @Nonnull
  public static Pair<ChangeType, Urn> effectiveDocumentIngestAuthorizationKey(
      @Nonnull ChangeType changeType, @Nonnull Urn urn, boolean exists) {
    if (isDocumentEntity(urn) && isUpdateLike(changeType) && !exists) {
      return Pair.of(ChangeType.CREATE_ENTITY, urn);
    }
    return Pair.of(changeType, urn);
  }

  public static boolean isUpdateLike(@Nonnull ChangeType changeType) {
    return changeType == ChangeType.CREATE
        || changeType == ChangeType.UPSERT
        || changeType == ChangeType.UPDATE
        || changeType == ChangeType.RESTATE
        || changeType == ChangeType.PATCH;
  }

  public static boolean canViewDocumentEntity(
      @Nonnull OperationContext opContext, @Nonnull Urn documentUrn) {
    return canViewDocumentEntity(opContext, opContext.getAspectRetriever(), documentUrn);
  }

  public static boolean canViewDocumentEntity(
      @Nonnull AuthorizationSession session,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Urn documentUrn) {
    if (!(session instanceof OperationContext)
        || !DOCUMENT_ENTITY_NAME.equals(documentUrn.getEntityType())) {
      return AuthUtil.canViewEntity(session, documentUrn);
    }
    return canViewDocumentEntities(
        (OperationContext) session, aspectRetriever, List.of(documentUrn));
  }

  private static boolean canViewDocumentEntities(
      @Nonnull OperationContext opContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Collection<Urn> documentUrns) {
    if (shortCircuitAllowView(opContext)) {
      return true;
    }
    Set<Urn> documentsRequiringBridgeAuthorization =
        documentUrns.stream()
            .distinct()
            .filter(documentUrn -> !AuthUtil.canViewEntity(opContext, documentUrn))
            .collect(Collectors.toSet());
    if (documentsRequiringBridgeAuthorization.isEmpty()) {
      return true;
    }

    Map<Urn, Map<String, Aspect>> aspects =
        aspectRetriever.getLatestAspectObjects(
            opContext,
            documentsRequiringBridgeAuthorization,
            Set.of(DOCUMENT_INFO_ASPECT_NAME, SUB_TYPES_ASPECT_NAME));
    return documentsRequiringBridgeAuthorization.stream()
        .allMatch(
            documentUrn -> {
              Map<String, Aspect> documentAspects = aspects.getOrDefault(documentUrn, Map.of());
              DocumentInfo documentInfo =
                  toDocumentInfo(documentAspects.get(DOCUMENT_INFO_ASPECT_NAME));
              SubTypes subTypes = toSubTypes(documentAspects.get(SUB_TYPES_ASPECT_NAME));
              return evaluateBridgeDocumentView(opContext, documentUrn, documentInfo, subTypes);
            });
  }

  /** Bridge-aware VIEW using already-loaded aspects (avoids a second fetch on GraphQL hydrate). */
  public static boolean canViewDocumentEntity(
      @Nonnull AuthorizationSession session,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Urn documentUrn,
      @Nullable DocumentInfo documentInfo,
      @Nullable SubTypes subTypes) {
    if (!(session instanceof OperationContext)
        || !DOCUMENT_ENTITY_NAME.equals(documentUrn.getEntityType())) {
      return AuthUtil.canViewEntity(session, documentUrn);
    }
    // aspectRetriever retained for signature parity with the fetch overload; aspects are preloaded.
    java.util.Objects.requireNonNull(aspectRetriever);
    OperationContext opContext = (OperationContext) session;
    if (shortCircuitAllowView(opContext) || AuthUtil.canViewEntity(opContext, documentUrn)) {
      return true;
    }
    return evaluateBridgeDocumentView(opContext, documentUrn, documentInfo, subTypes);
  }

  private static boolean shortCircuitAllowView(@Nonnull OperationContext opContext) {
    return opContext.isSystemAuth()
        || AuthUtil.isAuthorized(opContext, PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE);
  }

  private static boolean evaluateBridgeDocumentView(
      @Nonnull OperationContext opContext,
      @Nonnull Urn documentUrn,
      @Nullable DocumentInfo documentInfo,
      @Nullable SubTypes subTypes) {
    if (!isBridgeDocument(documentUrn, subTypes)) {
      return false;
    }
    Urn sourceUrn = resolveDocumentBridgeSourceUrn(documentUrn, documentInfo, subTypes);
    if (sourceUrn == null) {
      log.warn(
          "Bridge document {} has an unresolvable source entity; denying view to avoid fail-open",
          documentUrn);
      opContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(
                      DocumentAuthorizationUtils.class, BRIDGE_SOURCE_RESOLUTION_FAILED_METRIC, 1));
      return false;
    }
    return AuthUtil.canViewEntity(opContext, sourceUrn);
  }

  public static boolean isAuthorizedDocumentOperation(
      @Nonnull OperationContext opContext,
      @Nonnull ApiOperation operation,
      @Nonnull Urn documentUrn) {
    return isAuthorizedDocumentOperation(opContext, operation, List.of(documentUrn));
  }

  /**
   * Authorizes document API operations. READ uses bridge-aware view; other ops use the document
   * privilege map (CREATE/UPDATE/DELETE already OR {@code MANAGE_DOCUMENTS}).
   */
  public static boolean isAuthorizedDocumentOperation(
      @Nonnull OperationContext opContext,
      @Nonnull ApiOperation operation,
      @Nonnull Collection<Urn> documentUrns) {
    if (opContext.isSystemAuth()) {
      return true;
    }
    if (documentUrns.isEmpty()) {
      return true;
    }
    if (operation == READ) {
      return documentUrns.stream().allMatch(urn -> canViewDocumentEntity(opContext, urn));
    }
    return AuthUtil.isAuthorizedEntityUrns(opContext, operation, documentUrns)
        || AuthUtil.isAuthorized(opContext, PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE);
  }

  public static void assertAuthorizedDocumentOperation(
      @Nonnull OperationContext opContext,
      @Nonnull ApiOperation operation,
      @Nonnull Urn documentUrn) {
    if (!isAuthorizedDocumentOperation(opContext, operation, documentUrn)) {
      throw new ServiceAuthorizationException(
          String.format(
              "Unauthorized to %s document %s", operation.name().toLowerCase(), documentUrn));
    }
  }

  @Nullable
  public static Urn resolveDocumentBridgeSourceUrn(
      @Nonnull Urn entityUrn, @Nullable DocumentInfo documentInfo, @Nullable SubTypes subTypes) {
    if (documentInfo == null
        || !documentInfo.hasCustomProperties()
        || !documentInfo.getCustomProperties().containsKey(BRIDGE_TYPE_PROPERTY)
        || !isBridgeDocument(entityUrn, subTypes)) {
      return null;
    }
    try {
      String bridgeType = documentInfo.getCustomProperties().get(BRIDGE_TYPE_PROPERTY);
      String sourceUrn = documentInfo.getCustomProperties().get(BRIDGE_SOURCE_ENTITY_PROPERTY);
      if (sourceUrn != null) {
        Urn parsedSourceUrn = Urn.createFromString(sourceUrn);
        return bridgeType != null && bridgeType.equals(parsedSourceUrn.getEntityType())
            ? parsedSourceUrn
            : null;
      }
      if (DATASET_ENTITY_NAME.equals(bridgeType) && documentInfo.hasRelatedAssets()) {
        return documentInfo.getRelatedAssets().stream()
            .map(asset -> asset.getAsset())
            .filter(urn -> DATASET_ENTITY_NAME.equals(urn.getEntityType()))
            .findFirst()
            .orElse(null);
      }
    } catch (Exception e) {
      return null;
    }
    return null;
  }

  public static boolean isBridgeDocument(@Nonnull Urn entityUrn, @Nullable SubTypes subTypes) {
    return DOCUMENT_ENTITY_NAME.equals(entityUrn.getEntityType())
        && entityUrn.getEntityKey() != null
        && entityUrn.getEntityKey().size() > 0
        && String.valueOf(entityUrn.getEntityKey().get(0)).startsWith(BRIDGE_DOC_ID_PREFIX)
        && subTypes != null
        && subTypes.hasTypeNames()
        && subTypes.getTypeNames().contains(BRIDGE_DOCUMENT_SUBTYPE);
  }

  public static boolean isDocumentEntity(@Nonnull Urn urn) {
    return DOCUMENT_ENTITY_NAME.equals(urn.getEntityType());
  }

  /** Convenience aliases matching DocumentService call sites. */
  public static void assertCanCreate(
      @Nonnull OperationContext opContext, @Nonnull Urn documentUrn) {
    assertAuthorizedDocumentOperation(opContext, CREATE, documentUrn);
  }

  public static void assertCanView(@Nonnull OperationContext opContext, @Nonnull Urn documentUrn) {
    assertAuthorizedDocumentOperation(opContext, READ, documentUrn);
  }

  public static void assertCanUpdate(
      @Nonnull OperationContext opContext, @Nonnull Urn documentUrn) {
    assertAuthorizedDocumentOperation(opContext, UPDATE, documentUrn);
  }

  public static void assertCanDelete(
      @Nonnull OperationContext opContext, @Nonnull Urn documentUrn) {
    assertAuthorizedDocumentOperation(opContext, DELETE, documentUrn);
  }

  @Nullable
  private static DocumentInfo toDocumentInfo(@Nullable Aspect aspect) {
    if (aspect == null) {
      return null;
    }
    return RecordUtils.toRecordTemplate(DocumentInfo.class, aspect.data());
  }

  @Nullable
  private static SubTypes toSubTypes(@Nullable Aspect aspect) {
    if (aspect == null) {
      return null;
    }
    return RecordUtils.toRecordTemplate(SubTypes.class, aspect.data());
  }
}
