package io.datahubproject.openapi.controller;

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
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.template.common.GenericPatchTemplate;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SearchUtil;
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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
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
      Set<String> aspectNames,
      boolean withSystemMetadata,
      @Nullable String scrollId,
      boolean expandEmpty)
      throws URISyntaxException;

  protected List<E> buildEntityList(
      @Nonnull OperationContext opContext,
      List<Urn> urns,
      @Nullable Set<String> aspectNames,
      boolean withSystemMetadata,
      boolean expandEmpty)
      throws URISyntaxException {

    LinkedHashMap<Urn, Map<AspectSpec, Long>> aspectSpecMap =
        resolveAspectSpecs(
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
      @RequestParam(value = "pitKeepAlive", required = false, defaultValue = "5m")
          String pitKeepALive)
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
            opContext
                .withSearchFlags(flags -> DEFAULT_SEARCH_FLAGS)
                .withSearchFlags(flags -> flags.setSkipCache(skipCache))
                .withSearchFlags(flags -> flags.setIncludeSoftDeleted(includeSoftDelete)),
            List.of(entitySpec.getName()),
            query,
            null,
            sortCriteria,
            scrollId,
            pitKeepALive,
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
            mergedAspects,
            withSystemMetadata,
            result.getScrollId(),
            true));
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
              resolveAspectSpecs(
                  new LinkedHashMap<>(Map.of(urn, Map.of(aspectName, version))), 0L, true),
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

    AspectSpec aspectSpec = lookupAspectSpec(entitySpec, aspectName).get();
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
                .build(),
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
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestBody @Nonnull GenericJsonPatch patch)
      throws InvalidUrnException,
          NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {

    Urn urn = validatedUrn(entityUrn);
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "patchAspect", entityName),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, UPDATE, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + UPDATE + " entities.");
    }

    AspectSpec aspectSpec = lookupAspectSpec(entitySpec, aspectName).get();
    RecordTemplate currentValue = entityService.getAspect(opContext, urn, aspectSpec.getName(), 0);

    GenericPatchTemplate<? extends RecordTemplate> genericPatchTemplate =
        GenericPatchTemplate.builder()
            .genericJsonPatch(patch)
            .templateType(aspectSpec.getDataTemplateClass())
            .templateDefault(
                aspectSpec.getDataTemplateClass().getDeclaredConstructor().newInstance())
            .build();
    ChangeMCP upsert =
        toUpsertItem(
            opContext.getRetrieverContext().getAspectRetriever(),
            validatedUrn(entityUrn),
            aspectSpec,
            currentValue,
            genericPatchTemplate,
            authentication.getActor());

    List<UpdateAspectResult> results =
        entityService.ingestAspects(
            opContext,
            AspectsBatchImpl.builder()
                .retrieverContext(opContext.getRetrieverContext())
                .items(List.of(upsert))
                .build(),
            true,
            true);

    return results.stream()
        .findFirst()
        .map(result -> buildGenericEntity(aspectSpec.getName(), result, withSystemMetadata))
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().header(NOT_FOUND_HEADER, "ENTITY").build());
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

  /**
   * Given a map with aspect names from the API, normalized them into actual aspect names (casing
   * fixes)
   *
   * @param requestedAspectNames requested aspects
   * @param <T> map values
   * @param expandEmpty whether to expand empty aspect names to all aspect names
   * @return updated map
   */
  protected <T> LinkedHashMap<Urn, Map<AspectSpec, T>> resolveAspectSpecs(
      LinkedHashMap<Urn, Map<String, T>> requestedAspectNames,
      @Nonnull T defaultValue,
      boolean expandEmpty) {
    return requestedAspectNames.entrySet().stream()
        .map(
            entry -> {
              final Urn urn = entry.getKey();
              if (expandEmpty && (entry.getValue().isEmpty() || entry.getValue().containsKey(""))) {
                // All aspects specified
                Set<AspectSpec> allNames =
                    new HashSet<>(
                        entityRegistry.getEntitySpec(urn.getEntityType()).getAspectSpecs());
                return Map.entry(
                    urn,
                    allNames.stream()
                        .map(
                            aspectName ->
                                Map.entry(
                                    aspectName, entry.getValue().getOrDefault("", defaultValue)))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
              } else if (!entry.getValue().keySet().isEmpty()) {
                final Map<String, AspectSpec> normalizedNames =
                    entry.getValue().keySet().stream()
                        .map(
                            requestAspectName ->
                                Map.entry(
                                    requestAspectName, lookupAspectSpec(urn, requestAspectName)))
                        .filter(aspectSpecEntry -> aspectSpecEntry.getValue().isPresent())
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
                return Map.entry(
                    urn,
                    entry.getValue().entrySet().stream()
                        .filter(reqEntry -> normalizedNames.containsKey(reqEntry.getKey()))
                        .map(
                            reqEntry ->
                                Map.entry(
                                    normalizedNames.get(reqEntry.getKey()), reqEntry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
              } else {
                return (Map.Entry<Urn, Map<AspectSpec, T>>) null;
              }
            })
        .filter(Objects::nonNull)
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (a, b) -> {
                  throw new IllegalStateException("Duplicate key");
                },
                LinkedHashMap::new));
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
    return lookupAspectSpec(entityRegistry.getEntitySpec(urn.getEntityType()), aspectName);
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

  protected ChangeMCP toUpsertItem(
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Urn urn,
      @Nonnull AspectSpec aspectSpec,
      @Nullable RecordTemplate currentValue,
      @Nonnull GenericPatchTemplate<? extends RecordTemplate> genericPatchTemplate,
      @Nonnull Actor actor) {
    return ChangeItemImpl.fromPatch(
        urn,
        aspectSpec,
        currentValue,
        genericPatchTemplate,
        AuditStampUtils.createAuditStamp(actor.toUrnStr()),
        aspectRetriever);
  }

  /**
   * Case-insensitive fallback
   *
   * @return
   */
  protected static Optional<AspectSpec> lookupAspectSpec(EntitySpec entitySpec, String aspectName) {
    if (entitySpec == null) {
      return Optional.empty();
    }

    return entitySpec.getAspectSpec(aspectName) != null
        ? Optional.of(entitySpec.getAspectSpec(aspectName))
        : entitySpec.getAspectSpecs().stream()
            .filter(aspec -> aspec.getName().toLowerCase().equals(aspectName))
            .findFirst();
  }

  protected static Urn validatedUrn(String urn) throws InvalidUrnException {
    try {
      return Urn.createFromString(urn);
    } catch (URISyntaxException e) {
      throw new InvalidUrnException(urn, "Invalid urn!");
    }
  }
}
