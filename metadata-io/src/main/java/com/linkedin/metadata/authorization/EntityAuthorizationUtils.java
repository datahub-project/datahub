package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CHART_QUERY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_USAGE_STATISTICS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_TRANSFORM_LOGIC_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.QUERY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.VIEW_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
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
import java.util.HashSet;
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
    List<Urn> queries =
        urns.stream().filter(EntityAspectAuthorizationUtils::isQueryEntity).toList();
    List<Urn> others =
        urns.stream()
            .filter(urn -> !DocumentAuthorizationUtils.isDocumentEntity(urn))
            .filter(urn -> !EntityAspectAuthorizationUtils.isSchemaFieldEntity(urn))
            .filter(urn -> !EntityAspectAuthorizationUtils.isQueryEntity(urn))
            .toList();
    if (!others.isEmpty() && !AuthUtil.isAPIAuthorizedEntityUrns(opContext, apiOperation, others)) {
      return false;
    }
    if (!schemaFields.isEmpty()
        && !isAPIAuthorizedSchemaFieldUrns(opContext, apiOperation, schemaFields)) {
      return false;
    }
    if (!queries.isEmpty() && !isAPIAuthorizedQueryUrns(opContext, apiOperation, queries)) {
      return false;
    }
    return documents.isEmpty()
        || DocumentAuthorizationUtils.isAPIAuthorizedDocumentUrns(
            opContext, apiOperation, documents);
  }

  /**
   * Authorizes query entity URNs for API surfaces. READ requires {@code VIEW_ENTITY_QUERIES} (or a
   * privilege implying it) on the query's subject datasets via {@link
   * EntityAspectAuthorizationUtils#filterViewableQueryEntities} — active only when REST API
   * authorization is enabled AND query-read authorization is enabled (see {@link
   * EntityAspectAuthorizationUtils#isQueryViewAuthorizationEnabled}); when inactive, no subject
   * lookups are performed. System-auth requests bypass this filtering entirely, as the GraphQL
   * query paths already do. Query entities with no subjects are fail-closed. Other operations use
   * {@link AuthUtil} without inheritance.
   */
  private static boolean isAPIAuthorizedQueryUrns(
      @Nonnull OperationContext opContext,
      @Nonnull ApiOperation apiOperation,
      @Nonnull Collection<Urn> queryUrns) {
    if (queryUrns.isEmpty()) {
      return true;
    }
    if (apiOperation != READ) {
      return AuthUtil.isAPIAuthorizedEntityUrns(opContext, apiOperation, queryUrns);
    }
    return filterAPIAuthorizedQueryUrns(opContext, queryUrns).containsAll(queryUrns);
  }

  /**
   * Filters {@code queryUrns} down to the subset viewable by the actor, per {@link
   * EntityAspectAuthorizationUtils#filterViewableQueryEntities} — same activation conditions as
   * {@link #isAPIAuthorizedQueryUrns} (REST API authorization enabled, query-read authorization
   * enabled, not system auth), returning every urn unfiltered when any of those are inactive. Used
   * by result-set surfaces (search/scroll) that need to keep the queries the actor IS authorized to
   * see rather than rejecting the whole result because it also contains one they aren't — the same
   * all-or-nothing boolean above is correct for single-entity or batch existence-style checks,
   * where a mixed outcome should reasonably deny the caller's specific request.
   */
  @Nonnull
  public static Set<Urn> filterAPIAuthorizedQueryUrns(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> queryUrns) {
    if (queryUrns.isEmpty()
        || !AuthUtil.isRestApiAuthorizationEnabled()
        || !EntityAspectAuthorizationUtils.isQueryViewAuthorizationEnabled(opContext)
        || opContext.isSystemAuth()) {
      return new HashSet<>(queryUrns);
    }
    return EntityAspectAuthorizationUtils.filterViewableQueryEntities(
        opContext,
        opContext,
        opContext.getAspectRetriever(),
        queryUrns,
        EntityAspectAuthorizationUtils.requireAllQuerySubjects(opContext));
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
   * Whether {@code aspectName} on {@code entityUrn} carries SQL text ({@code viewProperties} on
   * datasets, {@code dataTransformLogic} on data jobs, {@code chartQuery} on charts — the same
   * fields GraphQL's {@code DatasetMapper}/{@code VersionedDatasetMapper}/{@code
   * DataTransformLogicMapper}/{@code ChartMapper} withhold) that the actor lacks {@link
   * EntityAspectAuthorizationUtils#canViewQueriesOnEntity} to see. OpenAPI (v1/v2/v3) and Rest.li
   * entity-read surfaces serialize aspect content generically — there is no per-field mapper the
   * way GraphQL has — so every one of them must consult this for these aspect names specifically
   * before including them in a response, withholding the whole aspect (coarser than GraphQL's
   * field-level redaction, which keeps non-SQL fields like {@code materialized}/{@code language} or
   * {@code type} visible) rather than not at all.
   */
  public static boolean isQuerySqlAspectRestricted(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn, @Nonnull String aspectName) {
    boolean isSqlBearingAspect =
        (DATASET_ENTITY_NAME.equals(entityUrn.getEntityType())
                && VIEW_PROPERTIES_ASPECT_NAME.equals(aspectName))
            || (DATA_JOB_ENTITY_NAME.equals(entityUrn.getEntityType())
                && DATA_TRANSFORM_LOGIC_ASPECT_NAME.equals(aspectName))
            || (CHART_ENTITY_NAME.equals(entityUrn.getEntityType())
                && CHART_QUERY_ASPECT_NAME.equals(aspectName));
    return isSqlBearingAspect
        && !EntityAspectAuthorizationUtils.canViewQueriesOnEntity(opContext, entityUrn);
  }

  /**
   * Removes SQL-bearing aspects (per {@link #isQuerySqlAspectRestricted}) from {@code responses} in
   * place — the shared redaction step for OpenAPI/Rest.li surfaces that fetch full {@link
   * EntityResponse} objects (v1 {@code EntitiesController}, Rest.li {@code EntityV2Resource});
   * surfaces that build a generic per-aspect map directly (v2/v3 {@code EntityController}) call
   * {@link #isQuerySqlAspectRestricted} themselves against their own map shape.
   */
  public static void completelyRedactUnauthorizedQuerySqlAspects(
      @Nonnull OperationContext opContext, @Nonnull Map<Urn, EntityResponse> responses) {
    for (Map.Entry<Urn, EntityResponse> entry : responses.entrySet()) {
      Urn urn = entry.getKey();
      EnvelopedAspectMap aspects = entry.getValue().getAspects();
      if (aspects == null) {
        continue;
      }
      for (String aspectName :
          List.of(
              VIEW_PROPERTIES_ASPECT_NAME,
              DATA_TRANSFORM_LOGIC_ASPECT_NAME,
              CHART_QUERY_ASPECT_NAME)) {
        if (aspects.containsKey(aspectName)
            && isQuerySqlAspectRestricted(opContext, urn, aspectName)) {
          aspects.remove(aspectName);
        }
      }
    }
  }

  /**
   * Whether {@code topSqlQueries} within a raw {@code datasetUsageStatistics} timeseries aspect for
   * {@code entityUrn} must be withheld from the actor. Unlike {@link #isQuerySqlAspectRestricted},
   * this only ever restricts one field within the aspect — the numeric usage counts alongside it
   * are never privilege-gated — so callers strip just that field (via {@link
   * #stripTopSqlQueriesFromRawAspect}) rather than dropping the whole aspect.
   */
  public static boolean isTopSqlQueriesFieldRestricted(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn, @Nonnull String aspectName) {
    return DATASET_ENTITY_NAME.equals(entityUrn.getEntityType())
        && DATASET_USAGE_STATISTICS_ASPECT_NAME.equals(aspectName)
        && EntityAspectAuthorizationUtils.isTopSqlQueriesRestricted(opContext, entityUrn);
  }

  /**
   * Removes {@code topSqlQueries} in place from a raw {@code datasetUsageStatistics} aspect map
   * when {@link #isTopSqlQueriesFieldRestricted} — the generic-read counterpart to {@code
   * UsageStats#stripTopSqlQueriesIfRestricted}, for surfaces (v3 {@code EntityController}'s
   * timeseries branch, GraphQL's raw-aspect resolver, OpenAPI v2 {@code TimeseriesController}) that
   * already hold the aspect as a live map — a Pegasus {@link DataMap} (itself a {@code Map<String,
   * Object>}) or, for the Elasticsearch-backed OpenAPI v2 path, the plain {@code Map<String,
   * Object>} parsed straight from the search hit's source — rather than {@link
   * com.linkedin.mxe.GenericAspect}-serialized bytes.
   */
  public static void stripTopSqlQueriesFromRawAspect(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull String aspectName,
      @Nonnull Map<String, Object> aspectValue) {
    if (isTopSqlQueriesFieldRestricted(opContext, entityUrn, aspectName)) {
      aspectValue.remove("topSqlQueries");
    }
  }

  /**
   * {@link #stripTopSqlQueriesFromRawAspect}'s counterpart for surfaces (Rest.li {@code
   * AspectResource#getTimeseriesAspectValues}, OpenAPI v2 {@code TimeseriesController}) that carry
   * timeseries aspect values as {@link com.linkedin.mxe.GenericAspect}-serialized bytes rather than
   * a live DataMap: deserializes, strips the field if restricted, and re-serializes back onto
   * {@code envelopedAspect} in place. A no-op re-serialization when nothing was stripped is
   * accepted for simplicity, since this only ever runs for {@code datasetUsageStatistics} values.
   */
  public static void stripTopSqlQueriesFromEnvelopedAspect(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull String aspectName,
      @Nonnull com.linkedin.metadata.aspect.EnvelopedAspect envelopedAspect) {
    if (!isTopSqlQueriesFieldRestricted(opContext, entityUrn, aspectName)) {
      return;
    }
    final com.linkedin.mxe.GenericAspect generic = envelopedAspect.getAspect();
    final com.linkedin.dataset.DatasetUsageStatistics stats =
        com.linkedin.metadata.utils.GenericRecordUtils.deserializeAspect(
            generic.getValue(),
            generic.getContentType(),
            com.linkedin.dataset.DatasetUsageStatistics.class);
    stats.data().remove("topSqlQueries");
    envelopedAspect.setAspect(
        com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect(stats));
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
          opContext,
          opContext,
          opContext.getAspectRetriever(),
          urn,
          EntityAspectAuthorizationUtils.requireAllQuerySubjects(opContext));
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
