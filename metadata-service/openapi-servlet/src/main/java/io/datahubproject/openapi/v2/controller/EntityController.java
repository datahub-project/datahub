package io.datahubproject.openapi.v2.controller;

import com.datahub.authentication.Actor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.entity.EntityApiUtils;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.controller.GenericEntitiesController;
import io.datahubproject.openapi.exception.InvalidUrnException;
import io.datahubproject.openapi.v2.models.GenericEntityScrollResultV2;
import io.datahubproject.openapi.v2.models.GenericEntityV2;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v2/entity")
@Slf4j
public class EntityController
    extends GenericEntitiesController<
        GenericEntityV2, GenericEntityScrollResultV2<GenericEntityV2>> {

  @Override
  public GenericEntityScrollResultV2<GenericEntityV2> buildScrollResult(
      @Nonnull OperationContext opContext,
      SearchEntityArray searchEntities,
      Set<String> aspectNames,
      boolean withSystemMetadata,
      @Nullable String scrollId)
      throws URISyntaxException {
    return GenericEntityScrollResultV2.<GenericEntityV2>builder()
        .results(toRecordTemplates(opContext, searchEntities, aspectNames, withSystemMetadata))
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

  @Override
  protected List<GenericEntityV2> buildEntityList(
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
                  GenericEntityV2.builder()
                      .urn(u.toString())
                      .build(
                          objectMapper,
                          toAspectMap(u, aspects.getOrDefault(u, List.of()), withSystemMetadata)))
          .collect(Collectors.toList());
    }
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

  private List<GenericEntityV2> toRecordTemplates(
      @Nonnull OperationContext opContext,
      SearchEntityArray searchEntities,
      Set<String> aspectNames,
      boolean withSystemMetadata)
      throws URISyntaxException {
    return buildEntityList(
        opContext,
        searchEntities.stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        aspectNames,
        withSystemMetadata);
  }

  @Override
  protected List<GenericEntityV2> buildEntityList(
      Set<IngestResult> ingestResults, boolean withSystemMetadata) {
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
              .build(objectMapper, aspectsMap));
    }
    return responseList;
  }
}
