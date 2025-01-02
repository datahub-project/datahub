package io.datahubproject.openapi.v3.controller;

import static com.linkedin.metadata.Constants.VERSION_SET_ENTITY_NAME;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.entity.EntityApiUtils;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.metadata.entity.versioning.VersionPropertiesInput;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.controller.GenericEntitiesController;
import io.datahubproject.openapi.exception.InvalidUrnException;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.v3.models.AspectItem;
import io.datahubproject.openapi.v3.models.GenericAspectV3;
import io.datahubproject.openapi.v3.models.GenericEntityAspectsBodyV3;
import io.datahubproject.openapi.v3.models.GenericEntityScrollResultV3;
import io.datahubproject.openapi.v3.models.GenericEntityV3;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController("EntityControllerV3")
@RequiredArgsConstructor
@RequestMapping("/openapi/v3/entity")
@Slf4j
@Hidden
public class EntityController
    extends GenericEntitiesController<
        GenericAspectV3, GenericEntityV3, GenericEntityScrollResultV3> {

  @Autowired private final EntityVersioningService entityVersioningService;
  @Autowired private final ConfigurationProvider configurationProvider;

  @Tag(name = "Generic Entities")
  @PostMapping(value = "/{entityName}/batchGet", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get a batch of entities")
  public ResponseEntity<List<GenericEntityV3>> getEntityBatch(
      HttpServletRequest httpServletRequest,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestBody @Nonnull String jsonEntityList)
      throws URISyntaxException, JsonProcessingException {

    LinkedHashMap<Urn, Map<AspectSpec, Long>> requestMap = toEntityVersionRequest(jsonEntityList);

    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(),
                    httpServletRequest,
                    "getEntityBatch",
                    requestMap.keySet().stream()
                        .map(Urn::getEntityType)
                        .collect(Collectors.toSet())),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, requestMap.keySet())) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + "  entities.");
    }

    return ResponseEntity.of(
        Optional.of(
            buildEntityVersionedAspectList(
                opContext, requestMap.keySet(), requestMap, withSystemMetadata, true)));
  }

  @Tag(name = "Generic Entities", description = "API for interacting with generic entities.")
  @PostMapping(value = "/scroll", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Scroll entities")
  public ResponseEntity<GenericEntityScrollResultV3> scrollEntities(
      HttpServletRequest request,
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
          Boolean includeSoftDelete,
      @RequestParam(value = "pitKeepAlive", required = false, defaultValue = "5m")
          String pitKeepALive,
      @RequestBody @Nonnull GenericEntityAspectsBodyV3 entityAspectsBody)
      throws URISyntaxException {

    final Collection<String> resolvedEntityNames;
    if (entityAspectsBody.getEntities() != null) {
      resolvedEntityNames =
          entityAspectsBody.getEntities().stream()
              .map(entityName -> entityRegistry.getEntitySpec(entityName))
              .map(EntitySpec::getName)
              .toList();
    } else {
      resolvedEntityNames =
          entityRegistry.getEntitySpecs().values().stream().map(EntitySpec::getName).toList();
    }

    Authentication authentication = AuthenticationContext.getAuthentication();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(),
                    request,
                    "scrollEntities",
                    resolvedEntityNames),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityType(opContext, READ, resolvedEntityNames)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + "  entities.");
    }

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
            resolvedEntityNames,
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

    return ResponseEntity.ok(
        buildScrollResult(
            opContext,
            result.getEntities(),
            entityAspectsBody.getAspects(),
            withSystemMetadata,
            result.getScrollId(),
            entityAspectsBody.getAspects() != null));
  }

  @Tag(name = "EntityVersioning")
  @PostMapping(
      value = "/versioning/{versionSetUrn}/relationship/versionOf/{entityUrn}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Link an Entity to a Version Set as the latest version")
  public ResponseEntity<List<GenericEntityV3>> linkLatestVersion(
      HttpServletRequest request,
      @PathVariable("versionSetUrn") String versionSetUrnString,
      @PathVariable("entityUrn") String entityUrnString,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata,
      @RequestBody @Nonnull VersionPropertiesInput versionPropertiesInput)
      throws URISyntaxException, JsonProcessingException {

    if (!configurationProvider.getFeatureFlags().isEntityVersioning()) {
      throw new IllegalAccessError(
          "Entity Versioning is not configured, please enable before attempting to use this feature.");
    }
    Authentication authentication = AuthenticationContext.getAuthentication();
    Urn versionSetUrn = UrnUtils.getUrn(versionSetUrnString);
    if (!VERSION_SET_ENTITY_NAME.equals(versionSetUrn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format("Version Set urn %s must be of type Version Set.", versionSetUrnString));
    }
    Urn entityUrn = UrnUtils.getUrn(entityUrnString);
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(),
                    request,
                    "linkLatestVersion",
                    ImmutableSet.of(entityUrn.getEntityType(), versionSetUrn.getEntityType())),
            authorizationChain,
            authentication,
            true);
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        opContext, UPDATE, ImmutableSet.of(versionSetUrn, entityUrn))) {
      throw new UnauthorizedException(
          String.format(
              "%s is unauthorized to %s entities %s and %s",
              authentication.getActor().toUrnStr(), UPDATE, versionSetUrnString, entityUrnString));
    }

    return ResponseEntity.ok(
        buildEntityList(
            opContext,
            entityVersioningService.linkLatestVersion(
                opContext, versionSetUrn, entityUrn, versionPropertiesInput),
            false));
  }

  @Tag(name = "EntityVersioning")
  @DeleteMapping(
      value = "/versioning/{versionSetUrn}/relationship/versionOf/{entityUrn}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Unlink the latest linked version of an entity")
  public ResponseEntity<List<String>> unlinkVersion(
      HttpServletRequest request,
      @PathVariable("versionSetUrn") String versionSetUrnString,
      @PathVariable("entityUrn") String entityUrnString,
      @RequestParam(value = "systemMetadata", required = false, defaultValue = "false")
          Boolean withSystemMetadata)
      throws URISyntaxException, JsonProcessingException {

    if (!configurationProvider.getFeatureFlags().isEntityVersioning()) {
      throw new IllegalAccessError(
          "Entity Versioning is not configured, please enable before attempting to use this feature.");
    }
    Authentication authentication = AuthenticationContext.getAuthentication();
    Urn versionSetUrn = UrnUtils.getUrn(versionSetUrnString);
    if (!VERSION_SET_ENTITY_NAME.equals(versionSetUrn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format("Version Set urn %s must be of type Version Set.", versionSetUrnString));
    }
    Urn entityUrn = UrnUtils.getUrn(entityUrnString);
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(),
                    request,
                    "unlinkVersion",
                    ImmutableSet.of(entityUrn.getEntityType(), versionSetUrn.getEntityType())),
            authorizationChain,
            authentication,
            true);
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        opContext, UPDATE, ImmutableSet.of(versionSetUrn, entityUrn))) {
      throw new UnauthorizedException(
          String.format(
              "%s is unauthorized to %s entities %s and %s",
              authentication.getActor().toUrnStr(), UPDATE, versionSetUrnString, entityUrnString));
    }
    List<RollbackResult> rollbackResults =
        entityVersioningService.unlinkVersion(opContext, versionSetUrn, entityUrn);

    return ResponseEntity.ok(
        rollbackResults.stream()
            .map(rollbackResult -> rollbackResult.getUrn().toString())
            .collect(Collectors.toList()));
  }

  @Override
  public GenericEntityScrollResultV3 buildScrollResult(
      @Nonnull OperationContext opContext,
      SearchEntityArray searchEntities,
      Set<String> aspectNames,
      boolean withSystemMetadata,
      @Nullable String scrollId,
      boolean expandEmpty)
      throws URISyntaxException {
    return GenericEntityScrollResultV3.builder()
        .entities(
            toRecordTemplates(
                opContext, searchEntities, aspectNames, withSystemMetadata, expandEmpty))
        .scrollId(scrollId)
        .build();
  }

  @Override
  protected List<GenericEntityV3> buildEntityVersionedAspectList(
      @Nonnull OperationContext opContext,
      Collection<Urn> requestedUrns,
      LinkedHashMap<Urn, Map<AspectSpec, Long>> urnAspectVersions,
      boolean withSystemMetadata,
      boolean expandEmpty)
      throws URISyntaxException {

    if (!urnAspectVersions.isEmpty()) {
      Map<Urn, List<EnvelopedAspect>> aspects =
          entityService.getEnvelopedVersionedAspects(
              opContext, aspectSpecsToAspectNames(urnAspectVersions, false), false);

      Map<Urn, Map<String, com.linkedin.metadata.aspect.EnvelopedAspect>> timeseriesAspects =
          aspectSpecsToAspectNames(urnAspectVersions, true).entrySet().stream()
              .map(
                  e -> {
                    // 0 is considered latest due to overlap with versioned and timeseries
                    Map<String, Long> endTimeMilliMap =
                        e.getValue().entrySet().stream()
                            .filter(endTEntry -> endTEntry.getValue() != 0L)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    return Map.entry(
                        e.getKey(),
                        timeseriesAspectService
                            .getLatestTimeseriesAspectValues(
                                opContext,
                                Set.of(e.getKey()),
                                e.getValue().keySet(),
                                endTimeMilliMap)
                            .getOrDefault(e.getKey(), Map.of()));
                  })
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      return urnAspectVersions.keySet().stream()
          .filter(
              urn ->
                  (aspects.containsKey(urn) && !aspects.get(urn).isEmpty())
                      || (timeseriesAspects.containsKey(urn)
                          && !timeseriesAspects.get(urn).isEmpty()))
          .map(
              u -> {
                Map<String, AspectItem> aspectItemMap = new HashMap<>();
                if (aspects.containsKey(u)) {
                  aspectItemMap.putAll(toAspectItemMap(u, aspects.get(u), withSystemMetadata));
                }
                if (timeseriesAspects.containsKey(u)) {
                  aspectItemMap.putAll(
                      toTimeseriesAspectItemMap(u, timeseriesAspects.get(u), withSystemMetadata));
                }

                return GenericEntityV3.builder().build(objectMapper, u, aspectItemMap);
              })
          .collect(Collectors.toList());
    } else if (!expandEmpty) {
      return requestedUrns.stream()
          .map(u -> GenericEntityV3.builder().build(objectMapper, u, Collections.emptyMap()))
          .collect(Collectors.toList());
    }

    return List.of();
  }

  private Map<String, AspectItem> toAspectItemMap(
      Urn urn, List<EnvelopedAspect> aspects, boolean withSystemMetadata) {
    return aspects.stream()
        .map(
            a ->
                Map.entry(
                    a.getName(),
                    AspectItem.builder()
                        .aspect(toRecordTemplate(lookupAspectSpec(urn, a.getName()).get(), a))
                        .systemMetadata(withSystemMetadata ? a.getSystemMetadata() : null)
                        .auditStamp(withSystemMetadata ? a.getCreated() : null)
                        .build()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<String, AspectItem> toTimeseriesAspectItemMap(
      Urn urn,
      Map<String, com.linkedin.metadata.aspect.EnvelopedAspect> aspects,
      boolean withSystemMetadata) {
    return aspects.entrySet().stream()
        .map(
            e ->
                Map.entry(
                    e.getKey(),
                    AspectItem.builder()
                        .aspect(
                            toRecordTemplate(lookupAspectSpec(urn, e.getKey()).get(), e.getValue()))
                        .systemMetadata(
                            withSystemMetadata ? e.getValue().getSystemMetadata() : null)
                        .build()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  protected List<GenericEntityV3> buildEntityList(
      OperationContext opContext,
      Collection<IngestResult> ingestResults,
      boolean withSystemMetadata) {
    List<GenericEntityV3> responseList = new LinkedList<>();

    Map<Urn, List<IngestResult>> entityMap =
        ingestResults.stream().collect(Collectors.groupingBy(IngestResult::getUrn));
    for (Map.Entry<Urn, List<IngestResult>> urnAspects : entityMap.entrySet()) {
      Map<String, AspectItem> aspectsMap =
          urnAspects.getValue().stream()
              .map(
                  ingest ->
                      Map.entry(
                          ingest.getRequest().getAspectName(),
                          AspectItem.builder()
                              .aspect(ingest.getRequest().getRecordTemplate())
                              .systemMetadata(
                                  withSystemMetadata
                                      ? ingest.getRequest().getSystemMetadata()
                                      : null)
                              .auditStamp(
                                  withSystemMetadata ? ingest.getRequest().getAuditStamp() : null)
                              .build()))
              // Map merge strategy, just take latest one
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, Map.Entry::getValue, (value1, value2) -> value2));
      responseList.add(
          GenericEntityV3.builder().build(objectMapper, urnAspects.getKey(), aspectsMap));
    }
    return responseList;
  }

  @Override
  protected GenericEntityV3 buildGenericEntity(
      @Nonnull String aspectName,
      @Nonnull UpdateAspectResult updateAspectResult,
      boolean withSystemMetadata) {
    return GenericEntityV3.builder()
        .build(
            objectMapper,
            updateAspectResult.getUrn(),
            Map.of(
                aspectName,
                AspectItem.builder()
                    .aspect(updateAspectResult.getNewValue())
                    .systemMetadata(
                        withSystemMetadata ? updateAspectResult.getNewSystemMetadata() : null)
                    .auditStamp(withSystemMetadata ? updateAspectResult.getAuditStamp() : null)
                    .build()));
  }

  @Override
  protected GenericEntityV3 buildGenericEntity(
      @Nonnull String aspectName, @Nonnull IngestResult ingestResult, boolean withSystemMetadata) {
    return GenericEntityV3.builder()
        .build(
            objectMapper,
            ingestResult.getUrn(),
            Map.of(
                aspectName,
                AspectItem.builder()
                    .aspect(ingestResult.getRequest().getRecordTemplate())
                    .systemMetadata(
                        withSystemMetadata ? ingestResult.getRequest().getSystemMetadata() : null)
                    .auditStamp(
                        withSystemMetadata ? ingestResult.getRequest().getAuditStamp() : null)
                    .build()));
  }

  private List<GenericEntityV3> toRecordTemplates(
      @Nonnull OperationContext opContext,
      SearchEntityArray searchEntities,
      Set<String> aspectNames,
      boolean withSystemMetadata,
      boolean expandEmpty)
      throws URISyntaxException {
    return buildEntityList(
        opContext,
        searchEntities.stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        aspectNames,
        withSystemMetadata,
        expandEmpty);
  }

  private LinkedHashMap<Urn, Map<AspectSpec, Long>> toEntityVersionRequest(
      @Nonnull String entityArrayList) throws JsonProcessingException, InvalidUrnException {
    JsonNode entities = objectMapper.readTree(entityArrayList);

    LinkedHashMap<Urn, Map<AspectSpec, Long>> items = new LinkedHashMap<>();
    if (entities.isArray()) {
      Iterator<JsonNode> entityItr = entities.iterator();
      while (entityItr.hasNext()) {
        JsonNode entity = entityItr.next();
        if (!entity.has("urn")) {
          throw new IllegalArgumentException("Missing `urn` field");
        }
        Urn entityUrn = validatedUrn(entity.get("urn").asText());
        items.putIfAbsent(entityUrn, new HashMap<>());

        Iterator<Map.Entry<String, JsonNode>> aspectItr = entity.fields();
        while (aspectItr.hasNext()) {
          Map.Entry<String, JsonNode> aspect = aspectItr.next();

          if ("urn".equals(aspect.getKey())) {
            continue;
          }

          AspectSpec aspectSpec = lookupAspectSpec(entityUrn, aspect.getKey()).orElse(null);

          if (aspectSpec != null) {

            Map<String, String> headers = null;
            if (aspect.getValue().has("headers")) {
              headers =
                  objectMapper.convertValue(
                      aspect.getValue().get("headers"), new TypeReference<>() {});
              items
                  .get(entityUrn)
                  .put(
                      aspectSpec,
                      Long.parseLong(headers.getOrDefault(HTTP_HEADER_IF_VERSION_MATCH, "0")));
            } else {
              items.get(entityUrn).put(aspectSpec, 0L);
            }
          }
        }

        // handle no aspects specified, default latest version
        if (items.get(entityUrn).isEmpty()) {
          for (AspectSpec aspectSpec :
              entityRegistry.getEntitySpec(entityUrn.getEntityType()).getAspectSpecs()) {
            items.get(entityUrn).put(aspectSpec, 0L);
          }
        }
      }
    }

    return items;
  }

  @Override
  protected AspectsBatch toMCPBatch(
      @Nonnull OperationContext opContext, String entityArrayList, Actor actor)
      throws JsonProcessingException, InvalidUrnException {
    JsonNode entities = objectMapper.readTree(entityArrayList);

    List<BatchItem> items = new LinkedList<>();
    if (entities.isArray()) {
      Iterator<JsonNode> entityItr = entities.iterator();
      while (entityItr.hasNext()) {
        JsonNode entity = entityItr.next();
        if (!entity.has("urn")) {
          throw new IllegalArgumentException("Missing `urn` field");
        }
        Urn entityUrn = validatedUrn(entity.get("urn").asText());

        Iterator<Map.Entry<String, JsonNode>> aspectItr = entity.fields();
        while (aspectItr.hasNext()) {
          Map.Entry<String, JsonNode> aspect = aspectItr.next();

          if ("urn".equals(aspect.getKey())) {
            continue;
          }

          AspectSpec aspectSpec = lookupAspectSpec(entityUrn, aspect.getKey()).orElse(null);
          SystemMetadata systemMetadata = null;
          if (aspect.getValue().has("systemMetadata")) {
            systemMetadata =
                EntityApiUtils.parseSystemMetadata(
                    objectMapper.writeValueAsString(aspect.getValue().get("systemMetadata")));
            ((ObjectNode) aspect.getValue()).remove("systemMetadata");
          }
          Map<String, String> headers = null;
          if (aspect.getValue().has("headers")) {
            headers =
                objectMapper.convertValue(
                    aspect.getValue().get("headers"), new TypeReference<>() {});
          }

          JsonNode jsonNodeAspect = aspect.getValue().get("value");

          if (opContext.getValidationContext().isAlternateValidation()) {
            ProposedItem.ProposedItemBuilder builder =
                ProposedItem.builder()
                    .metadataChangeProposal(
                        new MetadataChangeProposal()
                            .setEntityUrn(entityUrn)
                            .setAspectName(aspect.getKey())
                            .setEntityType(entityUrn.getEntityType())
                            .setChangeType(ChangeType.UPSERT)
                            .setAspect(GenericRecordUtils.serializeAspect(jsonNodeAspect))
                            .setHeaders(
                                headers != null ? new StringMap(headers) : null,
                                SetMode.IGNORE_NULL)
                            .setSystemMetadata(systemMetadata, SetMode.IGNORE_NULL))
                    .auditStamp(AuditStampUtils.createAuditStamp(actor.toUrnStr()))
                    .entitySpec(
                        opContext
                            .getAspectRetriever()
                            .getEntityRegistry()
                            .getEntitySpec(entityUrn.getEntityType()));
            items.add(builder.build());
          } else if (aspectSpec != null) {
            ChangeItemImpl.ChangeItemImplBuilder builder =
                ChangeItemImpl.builder()
                    .urn(entityUrn)
                    .aspectName(aspectSpec.getName())
                    .auditStamp(AuditStampUtils.createAuditStamp(actor.toUrnStr()))
                    .systemMetadata(systemMetadata)
                    .headers(headers)
                    .recordTemplate(
                        GenericRecordUtils.deserializeAspect(
                            ByteString.copyString(
                                objectMapper.writeValueAsString(jsonNodeAspect),
                                StandardCharsets.UTF_8),
                            GenericRecordUtils.JSON,
                            aspectSpec));

            items.add(builder.build(opContext.getRetrieverContext().getAspectRetriever()));
          }
        }
      }
    }
    return AspectsBatchImpl.builder()
        .items(items)
        .retrieverContext(opContext.getRetrieverContext())
        .build();
  }

  @Override
  protected ChangeMCP toUpsertItem(
      @Nonnull AspectRetriever aspectRetriever,
      Urn entityUrn,
      AspectSpec aspectSpec,
      Boolean createIfEntityNotExists,
      Boolean createIfNotExists,
      String jsonAspect,
      Actor actor)
      throws JsonProcessingException {
    JsonNode jsonNode = objectMapper.readTree(jsonAspect);
    String aspectJson = jsonNode.get("value").toString();

    final ChangeType changeType;
    if (Boolean.TRUE.equals(createIfEntityNotExists)) {
      changeType = ChangeType.CREATE_ENTITY;
    } else if (Boolean.TRUE.equals(createIfNotExists)) {
      changeType = ChangeType.CREATE;
    } else {
      changeType = ChangeType.UPSERT;
    }

    return ChangeItemImpl.builder()
        .urn(entityUrn)
        .aspectName(aspectSpec.getName())
        .changeType(changeType)
        .auditStamp(AuditStampUtils.createAuditStamp(actor.toUrnStr()))
        .recordTemplate(
            GenericRecordUtils.deserializeAspect(
                ByteString.copyString(aspectJson, StandardCharsets.UTF_8),
                GenericRecordUtils.JSON,
                aspectSpec))
        .build(aspectRetriever);
  }
}
