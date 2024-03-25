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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.template.common.GenericPatchTemplate;
import com.linkedin.metadata.entity.EntityService;
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
            .results(toRecordTemplates(result.getEntities(), aspectNames, withSystemMetadata))
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

    return ResponseEntity.of(
        Optional.of(
            BatchGetUrnResponse.builder()
                .entities(
                    new ArrayList<>(
                        toRecordTemplates(
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

    return ResponseEntity.of(
        toRecordTemplates(List.of(urn), aspectNames, withSystemMetadata).stream().findFirst());
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

    return exists(urn, null)
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
      @PathVariable("aspectName") String aspectName)
      throws URISyntaxException {

    Urn urn = UrnUtils.getUrn(entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        authentication, authorizationChain, READ, List.of(urn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + " entities.");
    }

    return ResponseEntity.of(
        toRecordTemplates(List.of(urn), Set.of(aspectName), true).stream()
            .findFirst()
            .flatMap(e -> e.getAspects().values().stream().findFirst()));
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

    return exists(urn, aspectName)
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

    entityService.deleteAspect(entityUrn, entitySpec.getKeyAspectName(), Map.of(), true);
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

    entityService.deleteAspect(entityUrn, aspectName, Map.of(), true);
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

    AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
    ChangeMCP upsert =
        toUpsertItem(urn, aspectSpec, createIfNotExists, jsonAspect, authentication.getActor());

    List<UpdateAspectResult> results =
        entityService.ingestAspects(
            AspectsBatchImpl.builder()
                .aspectRetriever(entityService)
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

    RecordTemplate currentValue = entityService.getAspect(urn, aspectName, 0);

    AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
    GenericPatchTemplate<? extends RecordTemplate> genericPatchTemplate =
        GenericPatchTemplate.builder()
            .genericJsonPatch(patch)
            .templateType(aspectSpec.getDataTemplateClass())
            .templateDefault(
                aspectSpec.getDataTemplateClass().getDeclaredConstructor().newInstance())
            .build();
    ChangeMCP upsert =
        toUpsertItem(
            UrnUtils.getUrn(entityUrn),
            aspectSpec,
            currentValue,
            genericPatchTemplate,
            authentication.getActor());

    List<UpdateAspectResult> results =
        entityService.ingestAspects(
            AspectsBatchImpl.builder()
                .aspectRetriever(entityService)
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

  private List<GenericEntity> toRecordTemplates(
      SearchEntityArray searchEntities, Set<String> aspectNames, boolean withSystemMetadata)
      throws URISyntaxException {
    return toRecordTemplates(
        searchEntities.stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        aspectNames,
        withSystemMetadata);
  }

  private Boolean exists(Urn urn, @Nullable String aspect) {
    return aspect == null
        ? entityService.exists(urn, true)
        : entityService.exists(urn, aspect, true);
  }

  private List<GenericEntity> toRecordTemplates(
      List<Urn> urns, Set<String> aspectNames, boolean withSystemMetadata)
      throws URISyntaxException {
    if (urns.isEmpty()) {
      return List.of();
    } else {
      Set<Urn> urnsSet = new HashSet<>(urns);

      Map<Urn, List<EnvelopedAspect>> aspects =
          entityService.getLatestEnvelopedAspects(
              urnsSet, resolveAspectNames(urnsSet, aspectNames));

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

  private Set<String> resolveAspectNames(Set<Urn> urns, Set<String> requestedNames) {
    if (requestedNames.isEmpty()) {
      return urns.stream()
          .flatMap(u -> entityRegistry.getEntitySpec(u.getEntityType()).getAspectSpecs().stream())
          .map(AspectSpec::getName)
          .collect(Collectors.toSet());
    } else {
      // ensure key is always present
      return Stream.concat(
              requestedNames.stream(),
              urns.stream()
                  .map(u -> entityRegistry.getEntitySpec(u.getEntityType()).getKeyAspectName()))
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
    return entityRegistry.getEntitySpec(urn.getEntityType()).getAspectSpec(aspectName);
  }

  private RecordTemplate toRecordTemplate(AspectSpec aspectSpec, EnvelopedAspect envelopedAspect) {
    return RecordUtils.toRecordTemplate(
        aspectSpec.getDataTemplateClass(), envelopedAspect.getValue().data());
  }

  private ChangeMCP toUpsertItem(
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
        .build(entityService);
  }

  private ChangeMCP toUpsertItem(
      @Nonnull Urn urn,
      @Nonnull AspectSpec aspectSpec,
      @Nullable RecordTemplate currentValue,
      @Nonnull GenericPatchTemplate<? extends RecordTemplate> genericPatchTemplate,
      @Nonnull Actor actor)
      throws URISyntaxException {
    return ChangeItemImpl.fromPatch(
        urn,
        aspectSpec,
        currentValue,
        genericPatchTemplate,
        AuditStampUtils.createAuditStamp(actor.toUrnStr()),
        entityService);
  }
}
