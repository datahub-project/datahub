package io.datahubproject.openapi.v3.controller;

import com.datahub.authentication.Actor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
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
import io.datahubproject.openapi.v3.models.GenericEntityScrollResultV3;
import io.datahubproject.openapi.v3.models.GenericEntityV3;
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

@RestController("EntityControllerV3")
@RequiredArgsConstructor
@RequestMapping("/v3/entity")
@Slf4j
public class EntityController
    extends GenericEntitiesController<
        GenericEntityV3, GenericEntityScrollResultV3<GenericEntityV3>> {

  @Override
  public GenericEntityScrollResultV3<GenericEntityV3> buildScrollResult(
      @Nonnull OperationContext opContext,
      SearchEntityArray searchEntities,
      Set<String> aspectNames,
      boolean withSystemMetadata,
      @Nullable String scrollId)
      throws URISyntaxException {
    return GenericEntityScrollResultV3.<GenericEntityV3>builder()
        .entities(toRecordTemplates(opContext, searchEntities, aspectNames, withSystemMetadata))
        .scrollId(scrollId)
        .build();
  }

  @Override
  protected List<GenericEntityV3> buildEntityList(
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
                  GenericEntityV3.builder()
                      .build(
                          objectMapper,
                          u,
                          toAspectMap(u, aspects.getOrDefault(u, List.of()), withSystemMetadata)))
          .collect(Collectors.toList());
    }
  }

  @Override
  protected List<GenericEntityV3> buildEntityList(
      Set<IngestResult> ingestResults, boolean withSystemMetadata) {
    List<GenericEntityV3> responseList = new LinkedList<>();

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
                Pair.of(
                    updateAspectResult.getNewValue(),
                    withSystemMetadata ? updateAspectResult.getNewSystemMetadata() : null)));
  }

  private List<GenericEntityV3> toRecordTemplates(
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
  protected AspectsBatch toMCPBatch(
      @Nonnull OperationContext opContext, String entityArrayList, Actor actor)
      throws JsonProcessingException {
    JsonNode entities = objectMapper.readTree(entityArrayList);

    List<BatchItem> items = new LinkedList<>();
    if (entities.isArray()) {
      Iterator<JsonNode> entityItr = entities.iterator();
      while (entityItr.hasNext()) {
        JsonNode entity = entityItr.next();
        if (!entity.has("urn")) {
          throw new IllegalArgumentException("Missing `urn` field");
        }
        Urn entityUrn = UrnUtils.getUrn(entity.get("urn").asText());

        Iterator<Map.Entry<String, JsonNode>> aspectItr = entity.fields();
        while (aspectItr.hasNext()) {
          Map.Entry<String, JsonNode> aspect = aspectItr.next();

          if ("urn".equals(aspect.getKey())) {
            continue;
          }

          AspectSpec aspectSpec = lookupAspectSpec(entityUrn, aspect.getKey());

          if (aspectSpec != null) {

            SystemMetadata systemMetadata = null;
            if (aspect.getValue().has("systemMetadata")) {
              systemMetadata =
                  EntityApiUtils.parseSystemMetadata(
                      objectMapper.writeValueAsString(aspect.getValue().get("systemMetadata")));
              ((ObjectNode) aspect.getValue()).remove("systemMetadata");
            }

            ChangeItemImpl.ChangeItemImplBuilder builder =
                ChangeItemImpl.builder()
                    .urn(entityUrn)
                    .aspectName(aspectSpec.getName())
                    .auditStamp(AuditStampUtils.createAuditStamp(actor.toUrnStr()))
                    .systemMetadata(systemMetadata)
                    .recordTemplate(
                        GenericRecordUtils.deserializeAspect(
                            ByteString.copyString(
                                objectMapper.writeValueAsString(aspect.getValue()),
                                StandardCharsets.UTF_8),
                            GenericRecordUtils.JSON,
                            aspectSpec));

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
}
