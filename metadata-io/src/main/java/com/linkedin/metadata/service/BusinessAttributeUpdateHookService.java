package com.linkedin.metadata.service;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;

import com.google.common.collect.ImmutableSet;
import com.linkedin.businessattribute.BusinessAttributes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BusinessAttributeUpdateHookService {
  private static final String BUSINESS_ATTRIBUTE_OF = "BusinessAttributeOf";

  private final UpdateIndicesService updateIndicesService;
  private final int relatedEntitiesCount;
  private final int getRelatedEntitiesBatchSize;

  public static final String TAG = "TAG";
  public static final String GLOSSARY_TERM = "GLOSSARY_TERM";
  public static final String DOCUMENTATION = "DOCUMENTATION";

  public BusinessAttributeUpdateHookService(
      @NonNull UpdateIndicesService updateIndicesService,
      @NonNull @Value("${businessAttribute.fetchRelatedEntitiesCount}") int relatedEntitiesCount,
      @NonNull @Value("${businessAttribute.fetchRelatedEntitiesBatchSize}") int relatedBatchSize) {
    this.updateIndicesService = updateIndicesService;
    this.relatedEntitiesCount = relatedEntitiesCount;
    this.getRelatedEntitiesBatchSize = relatedBatchSize;
  }

  public void handleChangeEvent(
      @NonNull final OperationContext opContext, @NonNull final PlatformEvent event) {
    final EntityChangeEvent entityChangeEvent =
        GenericRecordUtils.deserializePayload(
            event.getPayload().getValue(), EntityChangeEvent.class);

    if (!entityChangeEvent.getEntityType().equals(Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME)) {
      log.info("Skipping MCL event for entity:" + entityChangeEvent.getEntityType());
      return;
    }

    final Set<String> businessAttributeCategories =
        ImmutableSet.of(TAG, GLOSSARY_TERM, DOCUMENTATION);
    if (!businessAttributeCategories.contains(entityChangeEvent.getCategory())) {
      log.info("Skipping MCL event for category: " + entityChangeEvent.getCategory());
      return;
    }

    Urn urn = entityChangeEvent.getEntityUrn();
    log.info("Business Attribute update hook invoked for urn :" + urn);
    fetchRelatedEntities(
        opContext,
        urn,
        (batch, batchNumber) -> processBatch(opContext, batch, batchNumber),
        null,
        0,
        1);
  }

  private void fetchRelatedEntities(
      @NonNull final OperationContext opContext,
      @NonNull final Urn urn,
      @NonNull final BiConsumer<RelatedEntitiesScrollResult, Integer> resultConsumer,
      @Nullable String scrollId,
      int consumedEntityCount,
      int batchNumber) {
    GraphRetriever graph = opContext.getRetrieverContext().get().getGraphRetriever();

    RelatedEntitiesScrollResult result =
        graph.scrollRelatedEntities(
            null,
            newFilter("urn", urn.toString()),
            null,
            EMPTY_FILTER,
            Arrays.asList(BUSINESS_ATTRIBUTE_OF),
            newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
            Edge.EDGE_SORT_CRITERION,
            scrollId,
            getRelatedEntitiesBatchSize,
            null,
            null);
    resultConsumer.accept(result, batchNumber);
    consumedEntityCount = consumedEntityCount + result.getEntities().size();
    if (result.getScrollId() != null && consumedEntityCount < relatedEntitiesCount) {
      batchNumber = batchNumber + 1;
      fetchRelatedEntities(
          opContext, urn, resultConsumer, result.getScrollId(), consumedEntityCount, batchNumber);
    }
  }

  private void processBatch(
      @NonNull OperationContext opContext,
      @NonNull RelatedEntitiesScrollResult batch,
      int batchNumber) {
    AspectRetriever aspectRetriever = opContext.getRetrieverContext().get().getAspectRetriever();
    log.info("BA Update Batch {} started", batchNumber);
    Set<Urn> entityUrns =
        batch.getEntities().stream()
            .map(RelatedEntity::getUrn)
            .map(UrnUtils::getUrn)
            .collect(Collectors.toSet());

    Map<Urn, Map<String, Aspect>> entityAspectMap =
        aspectRetriever.getLatestAspectObjects(
            entityUrns, Set.of(Constants.BUSINESS_ATTRIBUTE_ASPECT));

    entityAspectMap.entrySet().stream()
        .filter(entry -> entry.getValue().containsKey(Constants.BUSINESS_ATTRIBUTE_ASPECT))
        .forEach(
            entry -> {
              final Urn entityUrn = entry.getKey();
              final Aspect aspect = entry.getValue().get(Constants.BUSINESS_ATTRIBUTE_ASPECT);

              updateIndicesService.handleChangeEvent(
                  opContext,
                  PegasusUtils.constructMCL(
                      null,
                      Constants.SCHEMA_FIELD_ENTITY_NAME,
                      entityUrn,
                      ChangeType.UPSERT,
                      Constants.BUSINESS_ATTRIBUTE_ASPECT,
                      opContext.getAuditStamp(),
                      new BusinessAttributes(aspect.data()),
                      null,
                      null,
                      null));
            });
    log.info("BA Update Batch {} completed", batchNumber);
  }
}
