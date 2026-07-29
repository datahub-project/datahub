package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SUB_TYPES_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
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
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntity;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteEntity;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
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
import org.apache.http.HttpStatus;

/**
 * Shared document authorization: privilege relationships for CRUD plus bridge-aware VIEW.
 *
 * <p>View decision:
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
   * Authorizes entity URNs for API surfaces (OpenAPI / RestLi). Non-document URNs use {@link
   * AuthUtil#isAPIAuthorizedEntityUrns}. Document READ uses the bridge-aware view evaluator when
   * REST API authorization is enabled. Document CREATE/UPDATE distinguish existing entities from
   * upsert-created entities so CREATE privileges cannot overwrite existing documents and UPDATE
   * privileges alone cannot create missing ones.
   */
  public static boolean isAPIAuthorizedEntityUrns(
      @Nonnull OperationContext opContext,
      @Nonnull ApiOperation apiOperation,
      @Nonnull Collection<Urn> urns) {
    List<Urn> documents =
        urns.stream().filter(DocumentAuthorizationUtils::isDocumentEntity).toList();
    List<Urn> others = urns.stream().filter(urn -> !isDocumentEntity(urn)).toList();
    if (!others.isEmpty() && !AuthUtil.isAPIAuthorizedEntityUrns(opContext, apiOperation, others)) {
      return false;
    }
    if (documents.isEmpty()) {
      return true;
    }
    if (!AuthUtil.isRestApiAuthorizationEnabled()) {
      return true;
    }
    if (apiOperation == UPDATE || apiOperation == CREATE) {
      Map<Urn, Boolean> existence =
          opContext.getAspectRetriever().entityExists(opContext, Set.copyOf(documents));
      List<Urn> existingDocuments =
          documents.stream().filter(urn -> existence.getOrDefault(urn, false)).toList();
      List<Urn> newDocuments =
          documents.stream().filter(urn -> !existence.getOrDefault(urn, false)).toList();
      return (existingDocuments.isEmpty()
              || AuthUtil.isAPIAuthorizedEntityUrns(opContext, UPDATE, existingDocuments))
          && (newDocuments.isEmpty()
              || AuthUtil.isAPIAuthorizedEntityUrns(opContext, CREATE, newDocuments));
    }
    if (apiOperation != READ) {
      return AuthUtil.isAPIAuthorizedEntityUrns(opContext, apiOperation, documents);
    }
    return canViewDocumentEntities(opContext, opContext.getAspectRetriever(), documents);
  }

  /**
   * Authorizes MCP ingest while treating an update-like proposal for a missing document as entity
   * creation. Other entity types and change types retain the standard {@link AuthUtil} behavior.
   */
  public static List<Pair<MetadataChangeProposal, Integer>> isAPIAuthorizedIngest(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Collection<MetadataChangeProposal> mcps) {
    if (!AuthUtil.isRestApiAuthorizationEnabled()) {
      return AuthUtil.isAPIAuthorized(opContext, ENTITY, entityRegistry, mcps);
    }

    List<Pair<MetadataChangeProposal, Pair<ChangeType, Urn>>> resolvedProposals =
        mcps.stream()
            .map(
                mcp -> {
                  Urn urn = mcp.getEntityUrn();
                  if (urn == null) {
                    urn =
                        EntityKeyUtils.getUrnFromProposal(
                            mcp,
                            entityRegistry.getEntitySpec(mcp.getEntityType()).getKeyAspectSpec());
                  }
                  return Pair.of(mcp, Pair.of(mcp.getChangeType(), urn));
                })
            .toList();

    Set<Urn> documentUpdateUrns =
        resolvedProposals.stream()
            .map(Pair::getSecond)
            .filter(
                changeUrn ->
                    DOCUMENT_ENTITY_NAME.equals(changeUrn.getSecond().getEntityType())
                        && isUpdateLike(changeUrn.getFirst()))
            .map(Pair::getSecond)
            .collect(Collectors.toSet());
    Map<Urn, Boolean> documentExistence =
        documentUpdateUrns.isEmpty()
            ? Map.of()
            : opContext.getAspectRetriever().entityExists(opContext, documentUpdateUrns);

    Map<Pair<ChangeType, Urn>, Pair<ChangeType, Urn>> effectiveAuthorizationKeys =
        resolvedProposals.stream()
            .map(Pair::getSecond)
            .distinct()
            .collect(
                Collectors.toMap(
                    changeUrn -> changeUrn,
                    changeUrn ->
                        DOCUMENT_ENTITY_NAME.equals(changeUrn.getSecond().getEntityType())
                                && isUpdateLike(changeUrn.getFirst())
                                && !documentExistence.getOrDefault(changeUrn.getSecond(), false)
                            ? Pair.of(ChangeType.CREATE_ENTITY, changeUrn.getSecond())
                            : changeUrn));

    Map<Pair<ChangeType, Urn>, Integer> authorizationResults =
        AuthUtil.isAPIAuthorizedUrns(
            opContext, ENTITY, Set.copyOf(effectiveAuthorizationKeys.values()));
    return resolvedProposals.stream()
        .map(
            proposal ->
                Pair.of(
                    proposal.getFirst(),
                    authorizationResults.getOrDefault(
                        effectiveAuthorizationKeys.get(proposal.getSecond()),
                        HttpStatus.SC_INTERNAL_SERVER_ERROR)))
        .toList();
  }

  private static boolean isUpdateLike(@Nonnull ChangeType changeType) {
    return changeType == ChangeType.CREATE
        || changeType == ChangeType.UPSERT
        || changeType == ChangeType.UPDATE
        || changeType == ChangeType.RESTATE
        || changeType == ChangeType.PATCH;
  }

  /**
   * Performs the entity-type gate used before search operations. Documents are deferred to the
   * result-level bridge-aware authorization because inherited access cannot be determined from the
   * entity type alone.
   */
  public static boolean isAPIAuthorizedSearchEntityTypes(
      @Nonnull OperationContext opContext, @Nonnull Collection<String> entityTypes) {
    List<String> nonDocumentEntityTypes =
        entityTypes.stream().filter(type -> !DOCUMENT_ENTITY_NAME.equals(type)).toList();
    return nonDocumentEntityTypes.isEmpty()
        || AuthUtil.isAPIAuthorizedEntityType(opContext, READ, nonDocumentEntityTypes);
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull OperationContext opContext, @Nonnull SearchResult result) {
    return isAPIAuthorizedEntityUrns(
        opContext,
        READ,
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull OperationContext opContext, @Nonnull ScrollResult result) {
    return isAPIAuthorizedEntityUrns(
        opContext,
        READ,
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull OperationContext opContext, @Nonnull AutoCompleteResult result) {
    return isAPIAuthorizedEntityUrns(
        opContext,
        READ,
        result.getEntities().stream().map(AutoCompleteEntity::getUrn).collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull OperationContext opContext, @Nonnull BrowseResult result) {
    return isAPIAuthorizedEntityUrns(
        opContext,
        READ,
        result.getEntities().stream().map(BrowseResultEntity::getUrn).collect(Collectors.toList()));
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
