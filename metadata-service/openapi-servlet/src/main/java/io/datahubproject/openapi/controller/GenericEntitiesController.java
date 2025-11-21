package io.datahubproject.openapi.controller;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.TIMESTAMP_MILLIS;
import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.authorization.ApiOperation.EXISTS;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.SliceOptions;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.timeseries.TimeseriesAspectBase;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.InvalidUrnException;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.models.GenericAspect;
import io.datahubproject.openapi.models.GenericEntity;
import io.datahubproject.openapi.models.GenericEntityScrollResult;
import io.datahubproject.openapi.util.RequestInputUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Slf4j
public abstract class GenericEntitiesController<
    A extends GenericAspect,
    E extends GenericEntity<A>,
    S extends GenericEntityScrollResult<A, E>> {
  public static final String NOT_FOUND_HEADER = "Not-Found-Reason";
  protected static final SearchFlags DEFAULT_SEARCH_FLAGS =
      new SearchFlags().setFulltext(false).setSkipAggregates(true).setSkipHighlighting(true);

  @Autowired protected EntityRegistry entityRegistry;
  @Autowired protected SearchService searchService;
  @Autowired protected EntityService<?> entityService;
  @Autowired protected TimeseriesAspectService timeseriesAspectService;
  @Autowired protected AuthorizerChain authorizationChain;
  @Autowired protected ObjectMapper objectMapper;
  @Autowired protected ConfigurationProvider configurationProvider;

  @Qualifier("systemOperationContext")
  @Autowired
  protected OperationContext systemOperationContext;

  /**
   * Returns scroll result entities
   *
   * @param searchEntities the entities to contain in the result
   * @param aspectNames the aspect names present
   * @param withSystemMetadata whether to include system metadata in the result
   * @param scrollId the pagination token
   * @param expandEmpty whether to expand an empty aspects list to all aspects
   * @return result containing entities/aspects
   * @throws URISyntaxException parsing error
   */
  protected abstract S buildScrollResult(
      @Nonnull OperationContext opContext,
      SearchEntityArray searchEntities,
      SearchResultMetadata searchResultMetadata,
      Set<String> aspectNames,
      boolean withSystemMetadata,
      @Nullable String scrollId,
      boolean expandEmpty,
      int totalCount,
      boolean includeScrollIdPerEntity)
      throws URISyntaxException;

  protected List<E> buildEntityList(
      @Nonnull OperationContext opContext,
      List<Urn> urns,
      @Nullable Set<String> aspectNames,
      boolean withSystemMetadata,
      boolean expandEmpty)
      throws URISyntaxException {

    LinkedHashMap<Urn, Map<AspectSpec, Long>> aspectSpecMap =
        RequestInputUtil.resolveAspectSpecs(
            entityRegistry,
            urns.stream()
                .map(
                    urn ->
                        Map.entry(
                            urn,
                            Optional.ofNullable(aspectNames).orElse(Set.of()).stream()
                                .map(aspectName -> Map.entry(aspectName, 0L))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> {
                          throw new IllegalStateException("Duplicate key");
                        },
                        LinkedHashMap::new)),
            0L,
            expandEmpty);

    return buildEntityVersionedAspectList(
        opContext, urns, aspectSpecMap, withSystemMetadata, expandEmpty);
  }

  /**
   * Build a list of entities for an API response
   *
   * @param opContext the operation context
   * @param requestedUrns list of urns requested
   * @param fetchUrnAspectVersions the map of urn to aspect name and version to fetch
   * @param withSystemMetadata whether to include system metadata in the response entity
   * @param expandEmpty whether to expand an empty aspects list to all aspects
   * @return entity responses
   * @throws URISyntaxException urn parsing error
   */
  protected abstract List<E> buildEntityVersionedAspectList(
      @Nonnull OperationContext opContext,
      Collection<Urn> requestedUrns,
      LinkedHashMap<Urn, Map<AspectSpec, Long>> fetchUrnAspectVersions,
      boolean withSystemMetadata,
      boolean expandEmpty)
      throws URISyntaxException;

  protected abstract List<E> buildEntityList(
      OperationContext opContext,
      Collection<IngestResult> ingestResults,
      boolean withSystemMetadata);

  protected abstract E buildGenericEntity(
      @Nonnull String aspectName,
      @Nonnull UpdateAspectResult updateAspectResult,
      boolean withSystemMetadata);

  protected abstract E buildGenericEntity(
      @Nonnull String aspectName, @Nonnull IngestResult ingestResult, boolean withSystemMetadata);

  protected abstract AspectsBatch toMCPBatch(
      @Nonnull OperationContext opContext, String entityArrayList, Actor actor)
      throws JsonProcessingException, InvalidUrnException;

  @Tag(name = "Generic Entities", description = "API for interacting with generic entities.")
  @GetMapping(value = "/{entityName}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Scroll entities")
  public ResponseEntity<S> getEntities(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @RequestParam(value = "aspectNames", defaultValue = "") Set<String> aspects1,
      @RequestParam(value = "aspects", defaultValue = "") Set<String> aspects2,
      @RequestParam(value = "count", defaultValue = "10") Integer count,
      @RequestParam(value = "query", defaultValue = "*") String query,
      @RequestParam(value = "scrollId", required = false) String scrollId,
      @RequestParam(value = "sort", required = false, defaultValue = "urn") @Deprecated
          String sortField,
      @RequestParam(value = "sortCriteria", required = false) List<String> sortFields,
      @RequestParam(value = "sortOrder", required = false, defaultValue = "ASCENDING")
          String sortOrder,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestParam(value = "skipCache", required = false, defaultValue = "false")
          Boolean skipCache,
      @RequestParam(value = "includeSoftDelete", required = false, defaultValue = "false")
          Boolean includeSoftDelete,
      @RequestParam(value = "sliceId", required = false) Integer sliceId,
      @RequestParam(value = "sliceMax", required = false) Integer sliceMax,
      @Parameter(
              schema = @Schema(nullable = true),
              description =
                  "Point In Time keep alive, accepts a time based string like \"5m\" for five minutes.")
          @RequestParam(value = "pitKeepAlive", required = false, defaultValue = "5m")
          String pitKeepAlive)
      throws URISyntaxException {

    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    Authentication authentication = AuthenticationContext.getAuthentication();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "getEntities", entityName),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityType(opContext, READ, entityName)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + "  entities.");
    }

    SortOrder finalSortOrder =
        SortOrder.valueOf(Optional.ofNullable(sortOrder).orElse("ASCENDING"));

    List<SortCriterion> sortCriteria;
    if (!CollectionUtils.isEmpty(sortFields)
        && sortFields.stream().anyMatch(StringUtils::isNotBlank)) {
      sortCriteria = new ArrayList<>();
      sortFields.stream()
          .filter(StringUtils::isNotBlank)
          .forEach(field -> sortCriteria.add(SearchUtil.sortBy(field, finalSortOrder)));
    } else if (StringUtils.isNotBlank(sortField)) {
      sortCriteria = Collections.singletonList(SearchUtil.sortBy(sortField, finalSortOrder));
    } else {
      sortCriteria = Collections.singletonList(SearchUtil.sortBy("urn", finalSortOrder));
    }

    ScrollResult result =
        searchService.scrollAcrossEntities(
            opContext.withSearchFlags(
                flags ->
                    DEFAULT_SEARCH_FLAGS
                        .setSkipCache(skipCache)
                        .setIncludeSoftDeleted(includeSoftDelete)
                        .setSliceOptions(
                            sliceId != null && sliceMax != null
                                ? new SliceOptions().setId(sliceId).setMax(sliceMax)
                                : null,
                            SetMode.IGNORE_NULL)),
            List.of(entitySpec.getName()),
            query,
            null,
            sortCriteria,
            scrollId,
            pitKeepAlive != null && pitKeepAlive.isEmpty() ? null : pitKeepAlive,
            count);

    if (!AuthUtil.isAPIAuthorizedResult(opContext, result)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + " entities.");
    }

    Set<String> mergedAspects =
        ImmutableSet.<String>builder().addAll(aspects1).addAll(aspects2).build();

    return ResponseEntity.ok(
        buildScrollResult(
            opContext,
            result.getEntities(),
            null,
            mergedAspects,
            withSystemMetadata,
            result.getScrollId(),
            true,
            result.getNumEntities(),
            false));
  }

  @Tag(name = "Generic Entities")
  @GetMapping(
      value = "/{entityName}/{entityUrn:urn:li:.+}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<E> getEntity(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @RequestParam(value = "aspectNames", defaultValue = "") Set<String> aspects1,
      @RequestParam(value = "aspects", defaultValue = "") Set<String> aspects2,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata)
      throws URISyntaxException {

    Urn urn = validatedUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "getEntity", entityName),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + " entities.");
    }

    return buildEntityList(
            opContext,
            List.of(urn),
            ImmutableSet.<String>builder().addAll(aspects1).addAll(aspects2).build(),
            withSystemMetadata,
            true)
        .stream()
        .findFirst()
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().header(NOT_FOUND_HEADER, "ENTITY").build());
  }

  @Tag(name = "Generic Entities")
  @RequestMapping(
      value = "/{entityName}/{entityUrn:urn:li:.+}",
      method = {RequestMethod.HEAD})
  @Operation(summary = "Entity exists")
  public ResponseEntity<Object> headEntity(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @PathVariable(value = "includeSoftDelete", required = false) Boolean includeSoftDelete)
      throws InvalidUrnException {

    Urn urn = validatedUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "headEntity", entityName),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, EXISTS, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + EXISTS + " entities.");
    }

    return exists(opContext, urn, null, includeSoftDelete)
        ? ResponseEntity.noContent().build()
        : ResponseEntity.notFound().build();
  }

  @Tag(name = "Generic Aspects", description = "API for generic aspects.")
  @GetMapping(
      value = "/{entityName}/{entityUrn:urn:li:.+}/{aspectName}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get an entity's generic aspect.")
  public ResponseEntity<A> getAspect(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @PathVariable("aspectName") String aspectName,
      @RequestParam(value = "version", required = false, defaultValue = "0") Long version,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata)
      throws URISyntaxException {

    Urn urn = validatedUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "getAspect", entityName),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + " entities.");
    }

    final List<E> resultList;
    if (version == 0) {
      resultList =
          buildEntityList(opContext, List.of(urn), Set.of(aspectName), withSystemMetadata, true);
    } else {
      resultList =
          buildEntityVersionedAspectList(
              opContext,
              List.of(urn),
              RequestInputUtil.resolveAspectSpecs(
                  entityRegistry,
                  new LinkedHashMap<>(Map.of(urn, Map.of(aspectName, version))),
                  0L,
                  true),
              withSystemMetadata,
              true);
    }

    return resultList.stream()
        .findFirst()
        .flatMap(
            e ->
                e.getAspects().entrySet().stream()
                    .filter(entry -> entry.getKey().equalsIgnoreCase(aspectName))
                    .map(Map.Entry::getValue)
                    .findFirst())
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().header(NOT_FOUND_HEADER, "ENTITY").build());
  }

  @Tag(name = "Generic Aspects")
  @RequestMapping(
      value = "/{entityName}/{entityUrn:urn:li:.+}/{aspectName}",
      method = {RequestMethod.HEAD})
  @Operation(summary = "Whether an entity aspect exists.")
  public ResponseEntity<Object> headAspect(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @PathVariable("aspectName") String aspectName,
      @PathVariable(value = "includeSoftDelete", required = false) Boolean includeSoftDelete)
      throws InvalidUrnException {

    Urn urn = validatedUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "headAspect", entityName),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, EXISTS, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + EXISTS + " entities.");
    }

    return lookupAspectSpec(urn, aspectName)
        .filter(aspectSpec -> exists(opContext, urn, aspectSpec.getName(), includeSoftDelete))
        .map(aspectSpec -> ResponseEntity.noContent().build())
        .orElse(ResponseEntity.notFound().build());
  }

  @Tag(name = "Generic Entities")
  @DeleteMapping(value = "/{entityName}/{entityUrn:urn:li:.+}")
  @Operation(summary = "Delete an entity")
  public void deleteEntity(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @RequestParam(value = "aspects", required = false, defaultValue = "") Set<String> aspects,
      @RequestParam(value = "clear", required = false, defaultValue = "false") boolean clear)
      throws InvalidUrnException {

    Urn urn = validatedUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "deleteEntity", entityName),
            authorizationChain,
            authentication,
            true);

    // Check domain-based authorization if feature flag is enabled
    if (configurationProvider.getFeatureFlags().isDomainBasedAuthorizationEnabled()) {
      checkDomainAuthorizationForEntity(opContext, urn, authentication.getActor().toUrnStr());
    }

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, DELETE, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + DELETE + " entities.");
    }

    EntitySpec entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());

    if (clear) {
      // remove all aspects, preserve entity by retaining key aspect
      aspects =
          entitySpec.getAspectSpecs().stream()
              .map(AspectSpec::getName)
              .filter(name -> !name.equals(entitySpec.getKeyAspectName()))
              .collect(Collectors.toSet());
    }

    if (aspects == null || aspects.isEmpty() || aspects.contains(entitySpec.getKeyAspectName())) {
      entityService.deleteUrn(opContext, urn);
    } else {
      aspects.stream()
          .map(aspectName -> lookupAspectSpec(urn, aspectName).get().getName())
          .forEach(
              aspectName ->
                  entityService.deleteAspect(opContext, entityUrn, aspectName, Map.of(), true));
    }
  }

  @Tag(name = "Generic Entities")
  @PostMapping(value = "/{entityName}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Create a batch of entities.")
  public ResponseEntity<List<E>> createEntity(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @RequestParam(value = "async", required = false, defaultValue = "true") Boolean async,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestBody @Nonnull String jsonEntityList)
      throws InvalidUrnException, JsonProcessingException {

    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "createEntity", entityName),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityType(opContext, CREATE, entityName)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + CREATE + " entities.");
    }

    AspectsBatch batch = toMCPBatch(opContext, jsonEntityList, authentication.getActor());

    // Check domain-based authorization if feature flag is enabled
    if (configurationProvider.getFeatureFlags().isDomainBasedAuthorizationEnabled()) {
      checkDomainAuthorizationForBatch(opContext, batch, authentication.getActor().toUrnStr());
    }

    List<IngestResult> results = entityService.ingestProposal(opContext, batch, async);

    if (!async) {
      return ResponseEntity.ok(buildEntityList(opContext, results, withSystemMetadata));
    } else {
      return ResponseEntity.accepted()
          .body(buildEntityList(opContext, results, withSystemMetadata));
    }
  }

  @Tag(name = "Generic Aspects")
  @DeleteMapping(value = "/{entityName}/{entityUrn:urn:li:.+}/{aspectName}")
  @Operation(summary = "Delete an entity aspect.")
  public void deleteAspect(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @PathVariable("aspectName") String aspectName)
      throws InvalidUrnException {

    Urn urn = validatedUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "deleteAspect", entityName),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, DELETE, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + DELETE + " entities.");
    }

    lookupAspectSpec(urn, aspectName)
        .ifPresent(
            aspectSpec -> {
              if (aspectSpec.isTimeseries()) {
                Map<Urn, Map<String, com.linkedin.metadata.aspect.EnvelopedAspect>> latestMap =
                    timeseriesAspectService.getLatestTimeseriesAspectValues(
                        opContext, Set.of(urn), Set.of(aspectSpec.getName()), null);
                com.linkedin.metadata.aspect.EnvelopedAspect latestAspect =
                    latestMap.getOrDefault(urn, Map.of()).get(aspectSpec.getName());
                if (latestAspect != null) {
                  Long latestTs =
                      new TimeseriesAspectBase(toRecordTemplate(aspectSpec, latestAspect).data())
                          .getTimestampMillis();
                  timeseriesAspectService.deleteAspectValues(
                      opContext,
                      urn.getEntityType(),
                      aspectSpec.getName(),
                      QueryUtils.newFilter(
                          CriterionUtils.buildCriterion(
                              TIMESTAMP_MILLIS, Condition.EQUAL, String.valueOf(latestTs))));
                }
              } else {
                entityService.deleteAspect(
                    opContext, entityUrn, aspectSpec.getName(), Map.of(), true);
              }
            });
  }

  @Tag(name = "Generic Aspects")
  @PostMapping(
      value = "/{entityName}/{entityUrn:urn:li:.+}/{aspectName}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Create an entity aspect.")
  public ResponseEntity<E> createAspect(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @PathVariable("aspectName") String aspectName,
      @RequestParam(value = "async", required = false, defaultValue = "false") Boolean async,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestParam(value = "createIfEntityNotExists", required = false, defaultValue = "false")
          Boolean createIfEntityNotExists,
      @RequestParam(value = "createIfNotExists", required = false, defaultValue = "true")
          Boolean createIfNotExists,
      @RequestBody @Nonnull String jsonAspect)
      throws URISyntaxException, JsonProcessingException {

    Urn urn = validatedUrn(entityUrn);
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "createAspect", entityName),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, CREATE, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + CREATE + " entities.");
    }

    // Check domain-based authorization if feature flag is enabled
    if (configurationProvider.getFeatureFlags().isDomainBasedAuthorizationEnabled()) {
      checkDomainAuthorizationForEntity(opContext, urn, authentication.getActor().toUrnStr());
    }

    AspectSpec aspectSpec = RequestInputUtil.lookupAspectSpec(entitySpec, aspectName).get();
    ChangeMCP upsert =
        toUpsertItem(
            opContext.getRetrieverContext().getAspectRetriever(),
            urn,
            aspectSpec,
            createIfEntityNotExists,
            createIfNotExists,
            jsonAspect,
            authentication.getActor());

    List<IngestResult> results =
        entityService.ingestProposal(
            opContext,
            AspectsBatchImpl.builder()
                .retrieverContext(opContext.getRetrieverContext())
                .items(List.of(upsert))
                .build(opContext),
            async);

    if (!async) {
      return ResponseEntity.of(
          results.stream()
              .filter(item -> aspectSpec.getName().equals(item.getRequest().getAspectName()))
              .findFirst()
              .map(
                  result ->
                      buildGenericEntity(
                          aspectSpec.getName(), result.getResult(), withSystemMetadata)));
    } else {
      return results.stream()
          .filter(item -> aspectSpec.getName().equals(item.getRequest().getAspectName()))
          .map(
              result ->
                  ResponseEntity.accepted()
                      .body(buildGenericEntity(aspectSpec.getName(), result, withSystemMetadata)))
          .findFirst()
          .orElse(ResponseEntity.accepted().build());
    }
  }

  @Tag(name = "Generic Aspects")
  @PatchMapping(
      value = "/{entityName}/{entityUrn:urn:li:.+}/{aspectName}",
      consumes = {"application/json-patch+json", MediaType.APPLICATION_JSON_VALUE},
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Patch an entity aspect. (Experimental)")
  public ResponseEntity<E> patchAspect(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @PathVariable("aspectName") String aspectName,
      @RequestParam(value = "async", required = false, defaultValue = "false") Boolean async,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestBody @Nonnull GenericJsonPatch patch)
      throws InvalidUrnException {

    Urn urn = validatedUrn(entityUrn);
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    Authentication authentication = AuthenticationContext.getAuthentication();
    Actor actor = authentication.getActor();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(actor.toUrnStr(), request, "patchAspect", entityName),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, UPDATE, List.of(urn))) {
      throw new UnauthorizedException(
          actor.toUrnStr() + " is unauthorized to " + UPDATE + " entities.");
    }

    // Check domain-based authorization if feature flag is enabled
    if (configurationProvider.getFeatureFlags().isDomainBasedAuthorizationEnabled()) {
      checkDomainAuthorizationForEntity(opContext, urn, actor.toUrnStr());
    }

    AspectSpec aspectSpec = RequestInputUtil.lookupAspectSpec(entitySpec, aspectName).get();

    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType(urn.getEntityType())
            .setAspectName(aspectSpec.getName())
            .setChangeType(ChangeType.PATCH)
            .setAspect(GenericRecordUtils.serializePatch(patch, objectMapper));

    IngestResult result =
        entityService.ingestProposal(
            opContext, mcp, AuditStampUtils.createAuditStamp(actor.toUrnStr()), async);

    if (result != null) {
      return ResponseEntity.ok(
          buildGenericEntity(aspectSpec.getName(), result, withSystemMetadata));
    } else {
      return ResponseEntity.notFound().header(NOT_FOUND_HEADER, "ENTITY").build();
    }
  }

  protected Boolean exists(
      @Nonnull OperationContext opContext,
      Urn urn,
      @Nullable String aspect,
      @Nullable Boolean includeSoftDelete) {
    return aspect == null
        ? entityService.exists(
            opContext, urn, includeSoftDelete != null ? includeSoftDelete : false)
        : entityService.exists(
            opContext, urn, aspect, includeSoftDelete != null ? includeSoftDelete : false);
  }

  protected static <T> LinkedHashMap<Urn, Map<String, T>> aspectSpecsToAspectNames(
      LinkedHashMap<Urn, Map<AspectSpec, T>> urnAspectSpecsMap, boolean timeseries) {
    return urnAspectSpecsMap.entrySet().stream()
        .map(
            e ->
                Map.entry(
                    e.getKey(),
                    e.getValue().entrySet().stream()
                        .filter(a -> timeseries == a.getKey().isTimeseries())
                        .map(a -> Map.entry(a.getKey().getName(), a.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (a, b) -> {
                  throw new IllegalStateException("Duplicate key");
                },
                LinkedHashMap::new));
  }

  protected Map<String, Pair<RecordTemplate, SystemMetadata>> toAspectMap(
      Urn urn, List<EnvelopedAspect> aspects, boolean withSystemMetadata) {
    return aspects.stream()
        .map(
            a ->
                Map.entry(
                    a.getName(),
                    Pair.of(
                        toRecordTemplate(lookupAspectSpec(urn, a.getName()).get(), a),
                        withSystemMetadata ? a.getSystemMetadata() : null)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  protected Optional<AspectSpec> lookupAspectSpec(Urn urn, String aspectName) {
    return RequestInputUtil.lookupAspectSpec(
        entityRegistry.getEntitySpec(urn.getEntityType()), aspectName);
  }

  protected RecordTemplate toRecordTemplate(
      AspectSpec aspectSpec, EnvelopedAspect envelopedAspect) {
    return RecordUtils.toRecordTemplate(
        aspectSpec.getDataTemplateClass(), envelopedAspect.getValue().data());
  }

  protected RecordTemplate toRecordTemplate(
      AspectSpec aspectSpec, com.linkedin.metadata.aspect.EnvelopedAspect envelopedAspect) {
    return GenericRecordUtils.deserializeAspect(
        envelopedAspect.getAspect().getValue(),
        envelopedAspect.getAspect().getContentType(),
        aspectSpec);
  }

  protected abstract ChangeMCP toUpsertItem(
      @Nonnull AspectRetriever aspectRetriever,
      Urn entityUrn,
      AspectSpec aspectSpec,
      Boolean createIfEntityNotExists,
      Boolean createIfNotExists,
      String jsonAspect,
      Actor actor)
      throws URISyntaxException, JsonProcessingException;

  protected static Urn validatedUrn(String urn) throws InvalidUrnException {
    try {
      return Urn.createFromString(urn);
    } catch (URISyntaxException e) {
      throw new InvalidUrnException(urn, "Invalid urn!");
    }
  }

  /**
   * Check domain-based authorization for a single entity. This method fetches the entity's current
   * domain (if any) and verifies the user has EDIT_ENTITY_DOMAINS privilege on that domain.
   *
   * @param opContext the operation context
   * @param entityUrn the URN of the entity to check
   * @param actorUrn the URN of the actor performing the operation
   * @throws UnauthorizedException if the user lacks domain permissions
   */
  private void checkDomainAuthorizationForEntity(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn, @Nonnull String actorUrn)
      throws UnauthorizedException {

    // Fetch the entity's current domain aspect
    com.linkedin.entity.EntityResponse entityResponse;
    try {
      entityResponse =
          entityService.getEntityV2(
              opContext,
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(DOMAINS_ASPECT_NAME));
    } catch (Exception e) {
      // If we can't fetch the entity, skip domain authorization check
      log.warn("Error fetching entity {} for domain authorization: {}", entityUrn, e.getMessage());
      return;
    }

    // Extract domain URNs from the entity
    Set<Urn> domainUrns = extractDomainsFromEntity(entityResponse);

    // If entity has domains, check domain-based authorization using subresources
    if (!domainUrns.isEmpty()) {
      boolean authorized =
          AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
              opContext, UPDATE, List.of(entityUrn), domainUrns);

      if (!authorized) {
        throw new UnauthorizedException(
            actorUrn + " is unauthorized to perform UPDATE on entities with domains " + domainUrns);
      }
    }
  }

  /**
   * Check domain-based authorization for a batch of entities. This method examines each MCP in the
   * batch, extracts domain information, and verifies the user has appropriate domain permissions.
   *
   * @param opContext the operation context
   * @param batch the batch of MCPs being ingested
   * @param actorUrn the URN of the actor performing the operation
   * @throws UnauthorizedException if the user lacks domain permissions
   */
  private void checkDomainAuthorizationForBatch(
      @Nonnull OperationContext opContext, @Nonnull AspectsBatch batch, @Nonnull String actorUrn)
      throws UnauthorizedException {

    // Group MCPs by entity URN to avoid duplicate checks
    Map<Urn, List<ChangeMCP>> mcpsByEntity = new HashMap<>();
    for (var item : batch.getItems()) {
      if (item instanceof ChangeMCP) {
        ChangeMCP changeMCP = (ChangeMCP) item;
        mcpsByEntity.computeIfAbsent(changeMCP.getUrn(), k -> new ArrayList<>()).add(changeMCP);
      }
    }

    // Check domain authorization for each entity
    for (Map.Entry<Urn, List<ChangeMCP>> entry : mcpsByEntity.entrySet()) {
      Urn entityUrn = entry.getKey();
      List<ChangeMCP> entityMcps = entry.getValue();

      // Check if any MCP is setting domains
      Set<Urn> proposedDomainUrns = extractDomainsFromMcps(entityMcps);

      // If domains are being set, check authorization using subresources
      if (!proposedDomainUrns.isEmpty()) {
        boolean authorized =
            AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                opContext, CREATE, List.of(entityUrn), proposedDomainUrns);

        if (!authorized) {
          throw new UnauthorizedException(
              actorUrn
                  + " is unauthorized to perform CREATE on entities with domains "
                  + proposedDomainUrns);
        }
      }
    }
  }

  /**
   * Extract domain URNs from an entity response.
   *
   * @param entityResponse the entity response containing domain aspect
   * @return set of domain URNs, empty if none found
   */
  @Nonnull
  private Set<Urn> extractDomainsFromEntity(
      @Nullable com.linkedin.entity.EntityResponse entityResponse) {
    if (entityResponse == null) {
      return Collections.emptySet();
    }

    com.linkedin.entity.EnvelopedAspect domainsAspect =
        entityResponse.getAspects().get(DOMAINS_ASPECT_NAME);

    if (domainsAspect == null) {
      return Collections.emptySet();
    }

    try {
      Domains domains = new Domains(domainsAspect.getValue().data());
      if (domains.hasDomains() && domains.getDomains() != null && !domains.getDomains().isEmpty()) {
        return new HashSet<>(domains.getDomains());
      }
    } catch (Exception e) {
      // If we can't parse the domains, skip the check
      log.warn("Error parsing domains from entity response: {}", e.getMessage());
    }

    return Collections.emptySet();
  }

  /**
   * Extract domain URNs from a list of MCPs for a single entity. Looks for Domains aspect in the
   * MCPs.
   *
   * @param mcps the list of MCPs for an entity
   * @return set of domain URNs if found, empty set otherwise
   */
  @Nonnull
  private Set<Urn> extractDomainsFromMcps(@Nonnull List<ChangeMCP> mcps) {
    for (ChangeMCP mcp : mcps) {
      if (DOMAINS_ASPECT_NAME.equals(mcp.getAspectName())) {
        try {
          RecordTemplate aspectData = mcp.getRecordTemplate();
          if (aspectData instanceof Domains) {
            Domains domains = (Domains) aspectData;
            if (domains.hasDomains()
                && domains.getDomains() != null
                && !domains.getDomains().isEmpty()) {
              return new HashSet<>(domains.getDomains());
            }
          }
        } catch (Exception e) {
          // If we can't parse the domains, skip the check
          log.warn("Error parsing domains from MCP: {}", e.getMessage());
        }
      }
    }
    return Collections.emptySet();
  }
}
