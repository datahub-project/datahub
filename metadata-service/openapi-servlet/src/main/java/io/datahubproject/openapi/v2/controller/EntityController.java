package io.datahubproject.openapi.v2.controller;

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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.template.common.GenericPatchTemplate;
import com.linkedin.metadata.entity.EntityApiUtils;
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
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.v2.models.BatchGetUrnRequest;
import io.datahubproject.openapi.v2.models.BatchGetUrnResponse;
import io.datahubproject.openapi.v2.models.GenericEntity;
import io.datahubproject.openapi.v2.models.GenericScrollResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v2/entity")
@Slf4j
public class EntityController {
  private static final SearchFlags DEFAULT_SEARCH_FLAGS =
      new SearchFlags().setFulltext(false).setSkipAggregates(true).setSkipHighlighting(true);
  @Autowired private EntityRegistry entityRegistry;
  @Autowired private SearchService searchService;
  @Autowired private EntityService<?> entityService;
  @Autowired private AuthorizerChain authorizationChain;
  @Autowired private ObjectMapper objectMapper;

  @Qualifier("systemOperationContext")
  @Autowired
  private OperationContext systemOperationContext;

  @Tag(name = "Generic Entities", description = "API for interacting with generic entities.")
  @GetMapping(value = "/{entityName}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Scroll entities")
  public ResponseEntity<GenericScrollResult<GenericEntity>> getEntities(
      @PathVariable("entityName") String entityName,
      @RequestParam(value = "aspectNames", defaultValue = "") Set<String> aspectNames,
      @RequestParam(value = "count", defaultValue = "10") Integer count,
      @RequestParam(value = "query", defaultValue = "*") String query,
      @RequestParam(value = "scrollId", required = false) String scrollId,
      @RequestParam(value = "sort", required = false, defaultValue = "urn") String sortField,
      @RequestParam(value = "sortOrder", required = false, defaultValue = "ASCENDING")
          String sortOrder,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata)
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
            RequestContext.builder().buildOpenapi("getEntities", entityName),
            authorizationChain,
            authentication,
            true);

    // TODO: support additional and multiple sort params
    SortCriterion sortCriterion = SearchUtil.sortBy(sortField, SortOrder.valueOf(sortOrder));

    ScrollResult result =
        searchService.scrollAcrossEntities(
            opContext.withSearchFlags(flags -> DEFAULT_SEARCH_FLAGS),
            List.of(entitySpec.getName()),
            query,
            null,
            sortCriterion,
            scrollId,
            null,
            count);

    if (!AuthUtil.isAPIAuthorizedResult(authentication, authorizationChain, result)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + " entities.");
    }

    return ResponseEntity.ok(
        GenericScrollResult.<GenericEntity>builder()
            .results(
                toRecordTemplates(opContext, result.getEntities(), aspectNames, withSystemMetadata))
            .scrollId(result.getScrollId())
            .build());
  }

  @Tag(name = "Generic Entities")
  @PostMapping(value = "/batch/{entityName}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get a batch of entities")
  public ResponseEntity<BatchGetUrnResponse> getEntityBatch(
      @PathVariable("entityName") String entityName, @RequestBody BatchGetUrnRequest request)
      throws URISyntaxException {

    List<Urn> urns = request.getUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    Authentication authentication = AuthenticationContext.getAuthentication();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(authentication, authorizationChain, READ, urns)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + "  entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("getEntityBatch", entityName),
            authorizationChain,
            authentication,
            true);

    return ResponseEntity.of(
        Optional.of(
            BatchGetUrnResponse.builder()
                .entities(
                    new ArrayList<>(
                        toRecordTemplates(
                            opContext,
                            urns,
                            new HashSet<>(request.getAspectNames()),
                            request.isWithSystemMetadata())))
                .build()));
  }

  @Tag(name = "Generic Entities")
  @GetMapping(
      value = "/{entityName}/{entityUrn:urn:li:.+}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<GenericEntity> getEntity(
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @RequestParam(value = "aspectNames", defaultValue = "") Set<String> aspectNames,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata)
      throws URISyntaxException {

    Urn urn = UrnUtils.getUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, READ, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("getEntity", entityName),
            authorizationChain,
            authentication,
            true);

    return ResponseEntity.of(
        toRecordTemplates(opContext, List.of(urn), aspectNames, withSystemMetadata).stream()
            .findFirst());
  }

  @Tag(name = "Generic Entities")
  @RequestMapping(
      value = "/{entityName}/{entityUrn}",
      method = {RequestMethod.HEAD})
  @Operation(summary = "Entity exists")
  public ResponseEntity<Object> headEntity(
      @PathVariable("entityName") String entityName, @PathVariable("entityUrn") String entityUrn) {

    Urn urn = UrnUtils.getUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, EXISTS, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + EXISTS + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("headEntity", entityName),
            authorizationChain,
            authentication,
            true);

    return exists(opContext, urn, null)
        ? ResponseEntity.noContent().build()
        : ResponseEntity.notFound().build();
  }

  @Tag(name = "Generic Aspects", description = "API for generic aspects.")
  @GetMapping(
      value = "/{entityName}/{entityUrn}/{aspectName}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get an entity's generic aspect.")
  public ResponseEntity<Object> getAspect(
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @PathVariable("aspectName") String aspectName,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata)
      throws URISyntaxException {

    Urn urn = UrnUtils.getUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, READ, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("getAspect", entityName),
            authorizationChain,
            authentication,
            true);

    return ResponseEntity.of(
        toRecordTemplates(opContext, List.of(urn), Set.of(aspectName), withSystemMetadata).stream()
            .findFirst()
            .flatMap(
                e ->
                    e.getAspects().entrySet().stream()
                        .filter(
                            entry ->
                                entry.getKey().equals(lookupAspectSpec(urn, aspectName).getName()))
                        .map(Map.Entry::getValue)
                        .findFirst()));
  }

  @Tag(name = "Generic Aspects")
  @RequestMapping(
      value = "/{entityName}/{entityUrn}/{aspectName}",
      method = {RequestMethod.HEAD})
  @Operation(summary = "Whether an entity aspect exists.")
  public ResponseEntity<Object> headAspect(
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @PathVariable("aspectName") String aspectName) {

    Urn urn = UrnUtils.getUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, EXISTS, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + EXISTS + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("headAspect", entityName),
            authorizationChain,
            authentication,
            true);

    return exists(opContext, urn, lookupAspectSpec(urn, aspectName).getName())
        ? ResponseEntity.noContent().build()
        : ResponseEntity.notFound().build();
  }

  @Tag(name = "Generic Entities")
  @DeleteMapping(value = "/{entityName}/{entityUrn}")
  @Operation(summary = "Delete an entity")
  public void deleteEntity(
      @PathVariable("entityName") String entityName, @PathVariable("entityUrn") String entityUrn) {

    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    Urn urn = UrnUtils.getUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, DELETE, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + DELETE + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("deleteEntity", entityName),
            authorizationChain,
            authentication,
            true);

    entityService.deleteAspect(opContext, entityUrn, entitySpec.getKeyAspectName(), Map.of(), true);
  }

  @Tag(name = "Generic Entities")
  @PostMapping(value = "/{entityName}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Create a batch of entities.")
  public ResponseEntity<List<GenericEntity>> createEntity(
      @PathVariable("entityName") String entityName,
      @RequestParam(value = "async", required = false, defaultValue = "true") Boolean async,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestBody @Nonnull String jsonEntityList)
      throws URISyntaxException, JsonProcessingException {

    Authentication authentication = AuthenticationContext.getAuthentication();

    if (!AuthUtil.isAPIAuthorizedEntityType(
        authentication, authorizationChain, CREATE, entityName)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + CREATE + " entities.");
    }

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("createEntity", entityName),
            authorizationChain,
            authentication,
            true);

    AspectsBatch batch = toMCPBatch(opContext, jsonEntityList, authentication.getActor());
    Set<IngestResult> results = entityService.ingestProposal(opContext, batch, async);

    if (!async) {
      return ResponseEntity.ok(toEntityListResponse(results, withSystemMetadata));
    } else {
      return ResponseEntity.accepted().body(toEntityListResponse(results, withSystemMetadata));
    }
  }

  @Tag(name = "Generic Aspects")
  @DeleteMapping(value = "/{entityName}/{entityUrn}/{aspectName}")
  @Operation(summary = "Delete an entity aspect.")
  public void deleteAspect(
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @PathVariable("aspectName") String aspectName) {

    Urn urn = UrnUtils.getUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, DELETE, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + DELETE + " entities.");
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("deleteAspect", entityName),
            authorizationChain,
            authentication,
            true);

    entityService.deleteAspect(
        opContext, entityUrn, lookupAspectSpec(urn, aspectName).getName(), Map.of(), true);
  }

  @Tag(name = "Generic Aspects")
  @PostMapping(
      value = "/{entityName}/{entityUrn}/{aspectName}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Create an entity aspect.")
  public ResponseEntity<GenericEntity> createAspect(
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @PathVariable("aspectName") String aspectName,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestParam(value = "createIfNotExists", required = false, defaultValue = "false")
          Boolean createIfNotExists,
      @RequestBody @Nonnull String jsonAspect)
      throws URISyntaxException {

    Urn urn = UrnUtils.getUrn(entityUrn);
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
            RequestContext.builder().buildOpenapi("createAspect", entityName),
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
            .map(
                result ->
                    GenericEntity.builder()
                        .urn(result.getUrn().toString())
                        .build(
                            objectMapper,
                            Map.of(
                                aspectName,
                                Pair.of(
                                    result.getNewValue(),
                                    withSystemMetadata ? result.getNewSystemMetadata() : null)))));
  }

  @Tag(name = "Generic Aspects")
  @PatchMapping(
      value = "/{entityName}/{entityUrn}/{aspectName}",
      consumes = "application/json-patch+json",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Patch an entity aspect. (Experimental)")
  public ResponseEntity<GenericEntity> patchAspect(
      @PathVariable("entityName") String entityName,
      @PathVariable("entityUrn") String entityUrn,
      @PathVariable("aspectName") String aspectName,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestBody @Nonnull GenericJsonPatch patch)
      throws URISyntaxException,
          NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {

    Urn urn = UrnUtils.getUrn(entityUrn);
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
            RequestContext.builder().buildOpenapi("patchAspect", entityName),
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
            UrnUtils.getUrn(entityUrn),
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

    return ResponseEntity.of(
        results.stream()
            .findFirst()
            .map(
                result ->
                    GenericEntity.builder()
                        .urn(result.getUrn().toString())
                        .build(
                            objectMapper,
                            Map.of(
                                aspectSpec.getName(),
                                Pair.of(
                                    result.getNewValue(),
                                    withSystemMetadata ? result.getNewSystemMetadata() : null)))));
  }

  private List<GenericEntity> toRecordTemplates(
      @Nonnull OperationContext opContext,
      SearchEntityArray searchEntities,
      Set<String> aspectNames,
      boolean withSystemMetadata)
      throws URISyntaxException {
    return toRecordTemplates(
        opContext,
        searchEntities.stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        aspectNames,
        withSystemMetadata);
  }

  private Boolean exists(@Nonnull OperationContext opContext, Urn urn, @Nullable String aspect) {
    return aspect == null
        ? entityService.exists(opContext, urn, true)
        : entityService.exists(opContext, urn, aspect, true);
  }

  private List<GenericEntity> toRecordTemplates(
      @Nonnull OperationContext opContext,
      List<Urn> urns,
      Set<String> aspectNames,
      boolean withSystemMetadata)
      throws URISyntaxException {
    if (urns.isEmpty()) {
      return List.of();
    } else {
      Set<Urn> urnsSet = new HashSet<>(urns);

      Map<Urn, List<EnvelopedAspect>> aspects =
          entityService.getLatestEnvelopedAspects(
              opContext,
              urnsSet,
              resolveAspectNames(urnsSet, aspectNames).stream()
                  .map(AspectSpec::getName)
                  .collect(Collectors.toSet()));

      return urns.stream()
          .map(
              u ->
                  GenericEntity.builder()
                      .urn(u.toString())
                      .build(
                          objectMapper,
                          toAspectMap(u, aspects.getOrDefault(u, List.of()), withSystemMetadata)))
          .collect(Collectors.toList());
    }
  }

  private Set<AspectSpec> resolveAspectNames(Set<Urn> urns, Set<String> requestedAspectNames) {
    if (requestedAspectNames.isEmpty()) {
      return urns.stream()
          .flatMap(u -> entityRegistry.getEntitySpec(u.getEntityType()).getAspectSpecs().stream())
          .collect(Collectors.toSet());
    } else {
      // ensure key is always present
      return Stream.concat(
              urns.stream()
                  .flatMap(
                      urn ->
                          requestedAspectNames.stream()
                              .map(aspectName -> lookupAspectSpec(urn, aspectName))),
              urns.stream()
                  .map(u -> entityRegistry.getEntitySpec(u.getEntityType()).getKeyAspectSpec()))
          .collect(Collectors.toSet());
    }
  }

  private Map<String, Pair<RecordTemplate, SystemMetadata>> toAspectMap(
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

  private AspectSpec lookupAspectSpec(Urn urn, String aspectName) {
    return lookupAspectSpec(entityRegistry.getEntitySpec(urn.getEntityType()), aspectName);
  }

  private RecordTemplate toRecordTemplate(AspectSpec aspectSpec, EnvelopedAspect envelopedAspect) {
    return RecordUtils.toRecordTemplate(
        aspectSpec.getDataTemplateClass(), envelopedAspect.getValue().data());
  }

  private ChangeMCP toUpsertItem(
      @Nonnull AspectRetriever aspectRetriever,
      Urn entityUrn,
      AspectSpec aspectSpec,
      Boolean createIfNotExists,
      String jsonAspect,
      Actor actor)
      throws URISyntaxException {
    return ChangeItemImpl.builder()
        .urn(entityUrn)
        .aspectName(aspectSpec.getName())
        .changeType(Boolean.TRUE.equals(createIfNotExists) ? ChangeType.CREATE : ChangeType.UPSERT)
        .auditStamp(AuditStampUtils.createAuditStamp(actor.toUrnStr()))
        .recordTemplate(
            GenericRecordUtils.deserializeAspect(
                ByteString.copyString(jsonAspect, StandardCharsets.UTF_8),
                GenericRecordUtils.JSON,
                aspectSpec))
        .build(aspectRetriever);
  }

  private ChangeMCP toUpsertItem(
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

  private AspectsBatch toMCPBatch(
      @Nonnull OperationContext opContext, String entityArrayList, Actor actor)
      throws JsonProcessingException, URISyntaxException {
    JsonNode entities = objectMapper.readTree(entityArrayList);

    List<BatchItem> items = new LinkedList<>();
    if (entities.isArray()) {
      Iterator<JsonNode> entityItr = entities.iterator();
      while (entityItr.hasNext()) {
        JsonNode entity = entityItr.next();
        Urn entityUrn = UrnUtils.getUrn(entity.get("urn").asText());

        Iterator<Map.Entry<String, JsonNode>> aspectItr = entity.get("aspects").fields();
        while (aspectItr.hasNext()) {
          Map.Entry<String, JsonNode> aspect = aspectItr.next();

          AspectSpec aspectSpec = lookupAspectSpec(entityUrn, aspect.getKey());

          if (aspectSpec != null) {
            ChangeItemImpl.ChangeItemImplBuilder builder =
                ChangeItemImpl.builder()
                    .urn(entityUrn)
                    .aspectName(aspectSpec.getName())
                    .auditStamp(AuditStampUtils.createAuditStamp(actor.toUrnStr()))
                    .recordTemplate(
                        GenericRecordUtils.deserializeAspect(
                            ByteString.copyString(
                                objectMapper.writeValueAsString(aspect.getValue().get("value")),
                                StandardCharsets.UTF_8),
                            GenericRecordUtils.JSON,
                            aspectSpec));

            if (aspect.getValue().has("systemMetadata")) {
              builder.systemMetadata(
                  EntityApiUtils.parseSystemMetadata(
                      objectMapper.writeValueAsString(aspect.getValue().get("systemMetadata"))));
            }

            items.add(builder.build(opContext.getRetrieverContext().get().getAspectRetriever()));
          }
        }
      }
    }

    return AspectsBatchImpl.builder()
        .items(items)
        .retrieverContext(opContext.getRetrieverContext().get())
        .build();
  }

  public List<GenericEntity> toEntityListResponse(
      Set<IngestResult> ingestResults, boolean withSystemMetadata) {
    List<GenericEntity> responseList = new LinkedList<>();

    Map<Urn, List<IngestResult>> entityMap =
        ingestResults.stream().collect(Collectors.groupingBy(IngestResult::getUrn));
    for (Map.Entry<Urn, List<IngestResult>> urnAspects : entityMap.entrySet()) {
      Map<String, Pair<RecordTemplate, SystemMetadata>> aspectsMap =
          urnAspects.getValue().stream()
              .map(
                  ingest ->
                      Map.entry(
                          ingest.getRequest().getAspectName(),
                          Pair.of(
                              ingest.getRequest().getRecordTemplate(),
                              withSystemMetadata ? ingest.getRequest().getSystemMetadata() : null)))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      responseList.add(
          GenericEntity.builder()
              .urn(urnAspects.getKey().toString())
              .build(objectMapper, aspectsMap));
    }
    return responseList;
  }

  /**
   * Case-insensitive fallback
   *
   * @return
   */
  private static AspectSpec lookupAspectSpec(EntitySpec entitySpec, String aspectName) {
    return entitySpec.getAspectSpec(aspectName) != null
        ? entitySpec.getAspectSpec(aspectName)
        : entitySpec.getAspectSpecs().stream()
            .filter(aspec -> aspec.getName().toLowerCase().equals(aspectName))
            .findFirst()
            .get();
  }
}
