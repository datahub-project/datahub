package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.QUERY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntity;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteEntity;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.DocumentAuthorizationUtils;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

/**
 * Generic entity authorization facade.
 *
 * <p>Privilege evaluation is shared; activation is not:
 *
 * <ul>
 *   <li>{@link #isAPIAuthorizedEntityUrns} — OpenAPI / Rest.li; active only when REST API
 *       authorization is enabled
 *   <li>{@link #canViewEntity} — shared privilege evaluator (documents bridge-aware, queries
 *       subject-derived). Callers decide activation: View Authorization wrappers for GraphQL
 *       redaction, REST API wrappers for OpenAPI/Rest.li, or unconditional use for explicit GraphQL
 *       document operations
 * </ul>
 *
 * <p>Default API behavior delegates to {@link AuthUtil}. Entity-specific utilities are consulted
 * only when a URN or MCP requires specialized authorization (currently documents; schema field API
 * READ inherits from the parent dataset; query and schema field VIEW are handled on search/view
 * paths).
 */
@Slf4j
public final class EntityAuthorizationUtils {

  private EntityAuthorizationUtils() {}

  /**
   * Authorizes entity URNs for API surfaces (OpenAPI / RestLi). Activation follows {@code
   * authorization.restApiAuthorization}. Document URNs use document-specific CREATE/UPDATE
   * existence classification and bridge-aware READ. Schema field READ inherits from the parent
   * dataset encoded in the URN (same candidate order as GraphQL VIEW), then falls back to a direct
   * grant on the schema field; other operations on schema fields use {@link AuthUtil}. All other
   * entity types use {@link AuthUtil}.
   *
   * <p>Independent of View Authorization: when REST API auth is enabled, READ is enforced even if
   * view/search access controls are disabled.
   */
  public static boolean isAPIAuthorizedEntityUrns(
      @Nonnull OperationContext opContext,
      @Nonnull ApiOperation apiOperation,
      @Nonnull Collection<Urn> urns) {
    List<Urn> documents =
        urns.stream().filter(DocumentAuthorizationUtils::isDocumentEntity).toList();
    List<Urn> schemaFields =
        urns.stream()
            .filter(urn -> !DocumentAuthorizationUtils.isDocumentEntity(urn))
            .filter(EntityAspectAuthorizationUtils::isSchemaFieldEntity)
            .toList();
    List<Urn> others =
        urns.stream()
            .filter(urn -> !DocumentAuthorizationUtils.isDocumentEntity(urn))
            .filter(urn -> !EntityAspectAuthorizationUtils.isSchemaFieldEntity(urn))
            .toList();
    if (!others.isEmpty() && !AuthUtil.isAPIAuthorizedEntityUrns(opContext, apiOperation, others)) {
      return false;
    }
    if (!schemaFields.isEmpty()
        && !isAPIAuthorizedSchemaFieldUrns(opContext, apiOperation, schemaFields)) {
      return false;
    }
    return documents.isEmpty()
        || DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
            opContext, apiOperation, documents);
  }

  /**
   * Authorizes schema field URNs for API surfaces. READ reuses parent-dataset inheritance via
   * {@link EntityAspectAuthorizationUtils#canViewSchemaFieldEntity} when REST API authorization is
   * enabled; other operations use {@link AuthUtil} without inheritance.
   */
  private static boolean isAPIAuthorizedSchemaFieldUrns(
      @Nonnull OperationContext opContext,
      @Nonnull ApiOperation apiOperation,
      @Nonnull Collection<Urn> schemaFieldUrns) {
    if (schemaFieldUrns.isEmpty()) {
      return true;
    }
    if (apiOperation != READ) {
      return AuthUtil.isAPIAuthorizedEntityUrns(opContext, apiOperation, schemaFieldUrns);
    }
    if (!AuthUtil.isRestApiAuthorizationEnabled()) {
      return true;
    }
    return schemaFieldUrns.stream()
        .allMatch(urn -> EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(opContext, urn));
  }

  /**
   * Authorizes MCP ingest. Applies existence-aware CREATE vs EDIT privilege selection for all
   * entity types, seeds proposed domains for domain-scoped create/edit matching, and retains
   * document-specific effective change-type remapping.
   */
  public static List<Pair<MetadataChangeProposal, Integer>> isAPIAuthorizedIngest(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Collection<MetadataChangeProposal> mcps) {
    if (!AuthUtil.isRestApiAuthorizationEnabled()) {
      return AuthUtil.isAPIAuthorized(opContext, ENTITY, entityRegistry, mcps);
    }

    DomainWriteAuthorizationUtils.seedProposedDomainsFromMcps(opContext, entityRegistry, mcps);

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

    return authorizeResolvedChangeUrns(opContext, resolvedProposals);
  }

  /**
   * Authorize each change in an AspectsBatch (OpenAPI createEntity path) with existence-aware
   * privileges and proposed-domain seeding.
   */
  public static List<Pair<BatchItem, Integer>> isAPIAuthorizedBatchItems(
      @Nonnull OperationContext opContext, @Nonnull Collection<? extends BatchItem> items) {
    if (!AuthUtil.isRestApiAuthorizationEnabled()) {
      return items.stream().map(item -> Pair.of((BatchItem) item, HttpStatus.SC_OK)).toList();
    }

    List<? extends BatchItem> orderedItems =
        items instanceof List ? (List<? extends BatchItem>) items : List.copyOf(items);
    DomainWriteAuthorizationUtils.seedProposedDomainsForApiAuth(
        opContext, opContext, opContext.getAspectRetriever(), orderedItems);

    List<Pair<BatchItem, Pair<ChangeType, Urn>>> resolved =
        items.stream()
            .map(item -> Pair.of((BatchItem) item, Pair.of(item.getChangeType(), item.getUrn())))
            .toList();

    return authorizeResolvedChangeUrns(opContext, resolved);
  }

  private static <T> List<Pair<T, Integer>> authorizeResolvedChangeUrns(
      @Nonnull OperationContext opContext, @Nonnull List<Pair<T, Pair<ChangeType, Urn>>> resolved) {

    Set<Urn> allUrns =
        resolved.stream().map(pair -> pair.getSecond().getSecond()).collect(Collectors.toSet());
    Map<Urn, Boolean> entityExists =
        allUrns.isEmpty()
            ? Map.of()
            : opContext.getAspectRetriever().entityExists(opContext, allUrns);

    Map<Pair<ChangeType, Urn>, Pair<ChangeType, Urn>> effectiveAuthorizationKeys =
        resolved.stream()
            .map(Pair::getSecond)
            .distinct()
            .collect(
                Collectors.toMap(
                    changeUrn -> changeUrn,
                    changeUrn ->
                        DocumentAuthorizationUtils.isDocumentEntity(changeUrn.getSecond())
                            ? DocumentAuthorizationUtils.effectiveDocumentIngestAuthorizationKey(
                                changeUrn.getFirst(),
                                changeUrn.getSecond(),
                                entityExists.getOrDefault(changeUrn.getSecond(), false))
                            : changeUrn));

    Map<Pair<ChangeType, Urn>, Integer> authorizationResults =
        AuthUtil.isAPIAuthorizedUrns(
            opContext, ENTITY, Set.copyOf(effectiveAuthorizationKeys.values()), entityExists);

    return resolved.stream()
        .map(
            proposal ->
                Pair.of(
                    proposal.getFirst(),
                    authorizationResults.getOrDefault(
                        effectiveAuthorizationKeys.get(proposal.getSecond()),
                        HttpStatus.SC_INTERNAL_SERVER_ERROR)))
        .toList();
  }

  /**
   * Entity-type gate used before search operations. Entity types that authorize only at the result
   * level (currently documents) are deferred; remaining types use {@link AuthUtil}.
   */
  public static boolean isAPIAuthorizedSearchEntityTypes(
      @Nonnull OperationContext opContext, @Nonnull Collection<String> entityTypes) {
    List<String> typeLevelEntityTypes =
        entityTypes.stream().filter(type -> !DOCUMENT_ENTITY_NAME.equals(type)).toList();
    return typeLevelEntityTypes.isEmpty()
        || AuthUtil.isAPIAuthorizedEntityType(opContext, READ, typeLevelEntityTypes);
  }

  /**
   * Type-level gate used before write request bodies have been converted to URNs. Documents are
   * deferred to {@link #isAPIAuthorizedEntityUrns}: their required operation depends on existence,
   * so an early CREATE or UPDATE check can require both privileges for a single write.
   */
  public static boolean isAPIAuthorizedWriteEntityTypes(
      @Nonnull OperationContext opContext,
      @Nonnull ApiOperation apiOperation,
      @Nonnull Collection<String> entityTypes) {
    List<String> typeLevelEntityTypes =
        entityTypes.stream().filter(type -> !DOCUMENT_ENTITY_NAME.equals(type)).toList();
    return typeLevelEntityTypes.isEmpty()
        || AuthUtil.isAPIAuthorizedEntityType(opContext, apiOperation, typeLevelEntityTypes);
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

  public static boolean isAPIAuthorizedResult(
      @Nonnull OperationContext opContext, @Nonnull LineageSearchResult result) {
    return isAPIAuthorizedEntityUrns(
        opContext,
        READ,
        result.getEntities().stream()
            .map(entity -> entity.getEntity())
            .collect(Collectors.toList()));
  }

  public static boolean isAPIAuthorizedResult(
      @Nonnull OperationContext opContext, @Nonnull LineageScrollResult result) {
    return isAPIAuthorizedEntityUrns(
        opContext,
        READ,
        result.getEntities().stream()
            .map(entity -> entity.getEntity())
            .collect(Collectors.toList()));
  }

  /**
   * Shared entity VIEW privilege evaluator. Not gated by View Authorization or REST API
   * authorization flags — callers wrap this when they need those activation switches.
   *
   * <p>Query entities inherit from subjects; schema fields inherit from their containing dataset;
   * documents use bridge-aware document VIEW; all others use {@link AuthUtil}.
   */
  public static boolean canViewEntity(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    if (QUERY_ENTITY_NAME.equals(urn.getEntityType())) {
      return EntityAspectAuthorizationUtils.canViewQueryEntity(
          opContext, opContext, opContext.getAspectRetriever(), urn);
    }
    if (DOCUMENT_ENTITY_NAME.equals(urn.getEntityType())) {
      return DocumentAuthorizationUtils.canViewDocumentEntity(opContext, urn);
    }
    if (SCHEMA_FIELD_ENTITY_NAME.equals(urn.getEntityType())) {
      return EntityAspectAuthorizationUtils.canViewSchemaFieldEntity(opContext, urn);
    }
    return AuthUtil.canViewEntity(opContext, urn);
  }
}
