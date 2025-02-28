package io.datahubproject.openapi.v2.controller;

import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
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
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.controller.GenericEntitiesController;
import io.datahubproject.openapi.exception.InvalidUrnException;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.v2.models.BatchGetUrnRequestV2;
import io.datahubproject.openapi.v2.models.BatchGetUrnResponseV2;
import io.datahubproject.openapi.v2.models.GenericAspectV2;
import io.datahubproject.openapi.v2.models.GenericEntityScrollResultV2;
import io.datahubproject.openapi.v2.models.GenericEntityV2;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
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
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/openapi/v2/entity")
@Slf4j
public class EntityController
    extends GenericEntitiesController<
        GenericAspectV2, GenericEntityV2, GenericEntityScrollResultV2> {

  @Tag(name = "Generic Entities")
  @PostMapping(value = "/batch/{entityName}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get a batch of entities")
  public ResponseEntity<BatchGetUrnResponseV2<GenericAspectV2, GenericEntityV2>> getEntityBatch(
      HttpServletRequest httpServletRequest,
      @PathVariable("entityName") String entityName,
      @RequestBody BatchGetUrnRequestV2 request)
      throws URISyntaxException {

    List<Urn> urns = request.getUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(),
                    httpServletRequest,
                    "getEntityBatch",
                    entityName),
            authorizationChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, urns)) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr() + " is unauthorized to " + READ + "  entities.");
    }

    return ResponseEntity.of(
        Optional.of(
            BatchGetUrnResponseV2.<GenericAspectV2, GenericEntityV2>builder()
                .entities(
                    new ArrayList<>(
                        buildEntityList(
                            opContext,
                            urns,
                            new HashSet<>(request.getAspectNames()),
                            request.isWithSystemMetadata(),
                            true)))
                .build()));
  }

  @Override
  public GenericEntityScrollResultV2 buildScrollResult(
      @Nonnull OperationContext opContext,
      SearchEntityArray searchEntities,
      Set<String> aspectNames,
      boolean withSystemMetadata,
      @Nullable String scrollId,
      boolean expandEmpty)
      throws URISyntaxException {
    return GenericEntityScrollResultV2.builder()
        .results(
            toRecordTemplates(opContext, searchEntities, aspectNames, withSystemMetadata, true))
        .scrollId(scrollId)
        .build();
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

        if (!entity.has("aspects")) {
          throw new IllegalArgumentException("Missing `aspects` field");
        }
        Iterator<Map.Entry<String, JsonNode>> aspectItr = entity.get("aspects").fields();
        while (aspectItr.hasNext()) {
          Map.Entry<String, JsonNode> aspect = aspectItr.next();

          AspectSpec aspectSpec = lookupAspectSpec(entityUrn, aspect.getKey()).get();
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
                            .setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata()))
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
                    .recordTemplate(
                        GenericRecordUtils.deserializeAspect(
                            ByteString.copyString(
                                objectMapper.writeValueAsString(jsonNodeAspect),
                                StandardCharsets.UTF_8),
                            GenericRecordUtils.JSON,
                            aspectSpec));

            if (aspect.getValue().has("systemMetadata")) {
              builder.systemMetadata(
                  SystemMetadataUtils.parseSystemMetadata(
                      objectMapper.writeValueAsString(aspect.getValue().get("systemMetadata"))));
            }

            items.add(builder.build(opContext.getAspectRetriever()));
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
  protected List<GenericEntityV2> buildEntityVersionedAspectList(
      @Nonnull OperationContext opContext,
      Collection<Urn> requestedUrns,
      LinkedHashMap<Urn, Map<AspectSpec, Long>> urnAspectVersions,
      boolean withSystemMetadata,
      boolean expandEmpty)
      throws URISyntaxException {

    Map<Urn, List<EnvelopedAspect>> aspects =
        entityService.getEnvelopedVersionedAspects(
            opContext, aspectSpecsToAspectNames(urnAspectVersions, false), true);

    return urnAspectVersions.keySet().stream()
        .map(
            u ->
                GenericEntityV2.builder()
                    .urn(u.toString())
                    .build(
                        objectMapper,
                        toAspectMap(u, aspects.getOrDefault(u, List.of()), withSystemMetadata)))
        .collect(Collectors.toList());
  }

  @Override
  protected GenericEntityV2 buildGenericEntity(
      @Nonnull String aspectName,
      @Nonnull UpdateAspectResult updateAspectResult,
      boolean withSystemMetadata) {
    return GenericEntityV2.builder()
        .urn(updateAspectResult.getUrn().toString())
        .build(
            objectMapper,
            Map.of(
                aspectName,
                Pair.of(
                    updateAspectResult.getNewValue(),
                    withSystemMetadata ? updateAspectResult.getNewSystemMetadata() : null)));
  }

  @Override
  protected GenericEntityV2 buildGenericEntity(
      @Nonnull String aspectName, @Nonnull IngestResult ingestResult, boolean withSystemMetadata) {
    return GenericEntityV2.builder()
        .urn(ingestResult.getUrn().toString())
        .build(
            objectMapper,
            Map.of(
                aspectName,
                Pair.of(
                    ingestResult.getRequest().getRecordTemplate(),
                    withSystemMetadata ? ingestResult.getRequest().getSystemMetadata() : null)));
  }

  private List<GenericEntityV2> toRecordTemplates(
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
        true);
  }

  protected List<GenericEntityV2> buildEntityList(
      OperationContext opContext,
      Collection<IngestResult> ingestResults,
      boolean withSystemMetadata) {
    List<GenericEntityV2> responseList = new LinkedList<>();

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
          GenericEntityV2.builder()
              .urn(urnAspects.getKey().toString())
              .build(
                  objectMapper,
                  aspectsMap,
                  opContext.getValidationContext().isAlternateValidation()));
    }
    return responseList;
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
      throws URISyntaxException {

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
                ByteString.copyString(jsonAspect, StandardCharsets.UTF_8),
                GenericRecordUtils.JSON,
                aspectSpec))
        .build(aspectRetriever);
  }
}
