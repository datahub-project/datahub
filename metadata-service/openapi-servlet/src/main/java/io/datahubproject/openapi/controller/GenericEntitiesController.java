package io.datahubproject.openapi.controller;

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
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.mxe.SystemMetadata;
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
   * @return result containing entities/aspects
   * @throws URISyntaxException parsing error
   */
  protected abstract S buildScrollResult(
      @Nonnull OperationContext opContext,
      SearchEntityArray searchEntities,
      Set<String> aspectNames,
      boolean withSystemMetadata,
      @Nullable String scrollId)
      throws URISyntaxException;

  protected List<E> buildEntityList(
      @Nonnull OperationContext opContext,
      List<Urn> urns,
      Set<String> aspectNames,
      boolean withSystemMetadata)
      throws URISyntaxException {

    LinkedHashMap<Urn, Map<String, Long>> versionMap =
        resolveAspectNames(
            urns.stream()
                .map(
                    urn ->
                        Map.entry(
                            urn,
                            aspectNames.stream()
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
            0L);
    return buildEntityVersionedAspectList(opContext, versionMap, withSystemMetadata);
  }

  protected abstract List<E> buildEntityVersionedAspectList(
      @Nonnull OperationContext opContext,
      LinkedHashMap<Urn, Map<String, Long>> urnAspectVersions,
      boolean withSystemMetadata)
      throws URISyntaxException;

  protected abstract List<E> buildEntityList(
      Set<IngestResult> ingestResults, boolean withSystemMetadata);

  protected abstract E buildGenericEntity(
      @Nonnull String aspectName,
      @Nonnull UpdateAspectResult updateAspectResult,
      boolean withSystemMetadata);

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
      @RequestParam(value = "sort", required = false, defaultValue = "urn") String sortField,
      @RequestParam(value = "sortCriteria", required = false) List<String> sortFields,
      @RequestParam(value = "sortOrder", required = false, defaultValue = "ASCENDING")
          String sortOrder,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestParam(value = "skipCache", required = false, defaultValue = "false")
          Boolean skipCache,
      @RequestParam(value = "includeSoftDelete", required = false, defaultValue = "false")
          Boolean includeSoftDelete)
      throws URISyntaxException {

    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    Authentication authentication = AuthenticationContext.getAuthentication();

    if (!AuthUtil.isAPIAuthorizedEntityType(authentication, authorizationChain, READ, entityName)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + "  entities.");
    }

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "getEntities", entityName),
            authorizationChain,
            authentication,
            true);

    List<SortCriterion> sortCriteria;
    if (!CollectionUtils.isEmpty(sortFields)) {
      sortCriteria = new ArrayList<>();
      sortFields.forEach(
          field -> sortCriteria.add(SearchUtil.sortBy(field, SortOrder.valueOf(sortOrder))));
    } else {
      sortCriteria =
          Collections.singletonList(SearchUtil.sortBy(sortField, SortOrder.valueOf(sortOrder)));
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
            null,
            count);

    if (!AuthUtil.isAPIAuthorizedResult(authentication, authorizationChain, result)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + " entities.");
    }

    return ResponseEntity.ok(
        buildScrollResult(
            opContext,
            result.getEntities(),
            ImmutableSet.<String>builder().addAll(aspects1).addAll(aspects2).build(),
            withSystemMetadata,
            result.getScrollId()));
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
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, READ, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "getEntity", entityName),
            authorizationChain,
            authentication,
            true);

    return buildEntityList(
            opContext,
            List.of(urn),
            ImmutableSet.<String>builder().addAll(aspects1).addAll(aspects2).build(),
            withSystemMetadata)
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
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, EXISTS, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + EXISTS + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "headEntity", entityName),
            authorizationChain,
            authentication,
            true);

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
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, READ, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "getAspect", entityName),
            authorizationChain,
            authentication,
            true);

    final List<E> resultList;
    if (version == 0) {
      resultList = buildEntityList(opContext, List.of(urn), Set.of(aspectName), withSystemMetadata);
    } else {
      resultList =
          buildEntityVersionedAspectList(
              opContext,
              new LinkedHashMap<>(Map.of(urn, Map.of(aspectName, version))),
              withSystemMetadata);
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
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, EXISTS, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + EXISTS + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "headAspect", entityName),
            authorizationChain,
            authentication,
            true);

    return exists(opContext, urn, lookupAspectSpec(urn, aspectName).getName(), includeSoftDelete)
        ? ResponseEntity.noContent().build()
        : ResponseEntity.notFound().build();
  }

  @Tag(name = "Generic Entities")
  @DeleteMapping(value = "/{entityName}/{entityUrn:urn:li:.+}")
  @Operation(summary = "Delete an entity")
  public void deleteEntity(
      HttpServletRequest request,
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn)
      throws InvalidUrnException {

    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    Urn urn = validatedUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, DELETE, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + DELETE + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "deleteEntity", entityName),
            authorizationChain,
            authentication,
            true);

    entityService.deleteUrn(opContext, urn);
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

    if (!AuthUtil.isAPIAuthorizedEntityType(
        authentication, authorizationChain, CREATE, entityName)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + CREATE + " entities.");
    }

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "createEntity", entityName),
            authorizationChain,
            authentication,
            true);

    AspectsBatch batch = toMCPBatch(opContext, jsonEntityList, authentication.getActor());
    Set<IngestResult> results = entityService.ingestProposal(opContext, batch, async);

    if (!async) {
      return ResponseEntity.ok(buildEntityList(results, withSystemMetadata));
    } else {
      return ResponseEntity.accepted().body(buildEntityList(results, withSystemMetadata));
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
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, DELETE, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + DELETE + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "deleteAspect", entityName),
            authorizationChain,
            authentication,
            true);

    entityService.deleteAspect(
        opContext, entityUrn, lookupAspectSpec(urn, aspectName).getName(), Map.of(), true);
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
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestParam(value = "createIfNotExists", required = false, defaultValue = "true")
          Boolean createIfNotExists,
      @RequestBody @Nonnull String jsonAspect)
      throws URISyntaxException, JsonProcessingException {

    Urn urn = validatedUrn(entityUrn);
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    Authentication authentication = AuthenticationContext.getAuthentication();

    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, CREATE, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + CREATE + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "createAspect", entityName),
            authorizationChain,
            authentication,
            true);

    AspectSpec aspectSpec = lookupAspectSpec(entitySpec, aspectName);
    ChangeMCP upsert =
        toUpsertItem(
            opContext.getRetrieverContext().get().getAspectRetriever(),
            urn,
            aspectSpec,
            createIfNotExists,
            jsonAspect,
            authentication.getActor());

    List<UpdateAspectResult> results =
        entityService.ingestAspects(
            opContext,
            AspectsBatchImpl.builder()
                .retrieverContext(opContext.getRetrieverContext().get())
                .items(List.of(upsert))
                .build(),
            true,
            true);

    return ResponseEntity.of(
        results.stream()
            .findFirst()
            .map(result -> buildGenericEntity(aspectName, result, withSystemMetadata)));
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
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, UPDATE, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + UPDATE + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "patchAspect", entityName),
            authorizationChain,
            authentication,
            true);

    AspectSpec aspectSpec = lookupAspectSpec(entitySpec, aspectName);
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
            opContext.getRetrieverContext().get().getAspectRetriever(),
            validatedUrn(entityUrn),
            aspectSpec,
            currentValue,
            genericPatchTemplate,
            authentication.getActor());

    List<UpdateAspectResult> results =
        entityService.ingestAspects(
            opContext,
            AspectsBatchImpl.builder()
                .retrieverContext(opContext.getRetrieverContext().get())
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
   * @return updated map
   */
  protected <T> LinkedHashMap<Urn, Map<String, T>> resolveAspectNames(
      LinkedHashMap<Urn, Map<String, T>> requestedAspectNames, @Nonnull T defaultValue) {
    return requestedAspectNames.entrySet().stream()
        .map(
            entry -> {
              final Urn urn = entry.getKey();
              if (entry.getValue().isEmpty() || entry.getValue().containsKey("")) {
                // All aspects specified
                Set<String> allNames =
                    entityRegistry.getEntitySpec(urn.getEntityType()).getAspectSpecs().stream()
                        .map(AspectSpec::getName)
                        .collect(Collectors.toSet());
                return Map.entry(
                    urn,
                    allNames.stream()
                        .map(
                            aspectName ->
                                Map.entry(
                                    aspectName, entry.getValue().getOrDefault("", defaultValue)))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
              } else {
                final Map<String, String> normalizedNames =
                    entry.getValue().keySet().stream()
                        .map(
                            requestAspectName ->
                                Map.entry(
                                    requestAspectName,
                                    lookupAspectSpec(urn, requestAspectName).getName()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return Map.entry(
                    urn,
                    entry.getValue().entrySet().stream()
                        .filter(reqEntry -> normalizedNames.containsKey(reqEntry.getKey()))
                        .map(
                            reqEntry ->
                                Map.entry(
                                    normalizedNames.get(reqEntry.getKey()), reqEntry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
              }
            })
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
                        toRecordTemplate(lookupAspectSpec(urn, a.getName()), a),
                        withSystemMetadata ? a.getSystemMetadata() : null)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  protected AspectSpec lookupAspectSpec(Urn urn, String aspectName) {
    return lookupAspectSpec(entityRegistry.getEntitySpec(urn.getEntityType()), aspectName);
  }

  protected RecordTemplate toRecordTemplate(
      AspectSpec aspectSpec, EnvelopedAspect envelopedAspect) {
    return RecordUtils.toRecordTemplate(
        aspectSpec.getDataTemplateClass(), envelopedAspect.getValue().data());
  }

  protected abstract ChangeMCP toUpsertItem(
      @Nonnull AspectRetriever aspectRetriever,
      Urn entityUrn,
      AspectSpec aspectSpec,
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
  protected static AspectSpec lookupAspectSpec(EntitySpec entitySpec, String aspectName) {
    return entitySpec.getAspectSpec(aspectName) != null
        ? entitySpec.getAspectSpec(aspectName)
        : entitySpec.getAspectSpecs().stream()
            .filter(aspec -> aspec.getName().toLowerCase().equals(aspectName))
            .findFirst()
            .get();
  }

  protected static Urn validatedUrn(String urn) throws InvalidUrnException {
    try {
      return Urn.createFromString(urn);
    } catch (URISyntaxException e) {
      throw new InvalidUrnException(urn, "Invalid urn!");
    }
  }
}
