package com.linkedin.metadata.service;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;

import com.google.common.collect.ImmutableSet;
import com.linkedin.businessattribute.BusinessAttributes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BusinessAttributeUpdateHookService {
  private static final String BUSINESS_ATTRIBUTE_OF = "BusinessAttributeOf";
  private final SystemEntityClient systemEntityClient;

  private final int relatedEntitiesCount;
  private final int getRelatedEntitiesBatchSize;

  public static final String TAG = "TAG";
  public static final String GLOSSARY_TERM = "GLOSSARY_TERM";
  public static final String DOCUMENTATION = "DOCUMENTATION";

  public BusinessAttributeUpdateHookService(
      @NonNull @Qualifier("systemEntityClient") SystemEntityClient systemEntityClient,
      @NonNull @Value("${businessAttribute.fetchRelatedEntitiesCount}") int relatedEntitiesCount,
      @NonNull @Value("${businessAttribute.fetchRelatedEntitiesBatchSize}") int relatedBatchSize) {
    this.systemEntityClient = systemEntityClient;
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

    Consumer<RelatedEntitiesScrollResult> consumer =
        relatedEntitiesScrollResult -> {
          try {
            Set<Urn> entityUrns =
                relatedEntitiesScrollResult.getEntities().stream()
                    .map(RelatedEntity::getUrn)
                    .map(UrnUtils::getUrn)
                    .collect(Collectors.toSet());

            Map<Urn, EntityResponse> entityResponseMap =
                systemEntityClient.batchGetV2(
                    opContext, entityUrns, Set.of(Constants.BUSINESS_ATTRIBUTE_ASPECT));

            List<MetadataChangeProposal> mcps =
                entityResponseMap.entrySet().stream()
                    .filter(
                        entry ->
                            entry
                                .getValue()
                                .getAspects()
                                .containsKey(Constants.BUSINESS_ATTRIBUTE_ASPECT))
                    .map(
                        entry -> {
                          EnvelopedAspect envelopedAspect =
                              entry
                                  .getValue()
                                  .getAspects()
                                  .get(Constants.BUSINESS_ATTRIBUTE_ASPECT);
                          BusinessAttributes businessAttributes =
                              new BusinessAttributes(envelopedAspect.getValue().data());
                          return new MetadataChangeProposal()
                              .setChangeType(ChangeType.UPSERT)
                              .setEntityType(Constants.SCHEMA_FIELD_ENTITY_NAME)
                              .setEntityUrn(entry.getKey())
                              .setAspectName(Constants.BUSINESS_ATTRIBUTE_ASPECT)
                              .setAspect(GenericRecordUtils.serializeAspect(businessAttributes))
                              .setSystemMetadata(
                                  envelopedAspect.getSystemMetadata(), SetMode.IGNORE_NULL);
                        })
                    .collect(Collectors.toList());

            if (!mcps.isEmpty()) {
              systemEntityClient.batchIngestProposals(opContext, mcps, true);
            }
          } catch (RemoteInvocationException | URISyntaxException e) {
            throw new RuntimeException(e);
          }
        };

    fetchRelatedEntities(opContext, urn, consumer, null, 0);
  }

  private void fetchRelatedEntities(
      @NonNull final OperationContext opContext,
      @NonNull final Urn urn,
      @NonNull final Consumer<RelatedEntitiesScrollResult> resultConsumer,
      @Nullable String scrollId,
      int consumedEntityCount) {
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
    resultConsumer.accept(result);

    if (result.getScrollId() != null && consumedEntityCount < relatedEntitiesCount) {
      fetchRelatedEntities(
          opContext,
          urn,
          resultConsumer,
          result.getScrollId(),
          consumedEntityCount + result.getEntities().size());
    }
  }
}
