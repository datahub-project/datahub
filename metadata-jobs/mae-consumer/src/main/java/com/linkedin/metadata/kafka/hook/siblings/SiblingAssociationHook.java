package com.linkedin.metadata.kafka.hook.siblings;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Siblings;
import com.linkedin.common.SubTypes;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/** This hook associates dbt datasets with their sibling entities */
@Slf4j
@Component
@Import({
  EntityRegistryFactory.class,
  RestliEntityClientFactory.class,
  EntitySearchServiceFactory.class
})
public class SiblingAssociationHook implements MetadataChangeLogHook {

  public static final String SIBLING_ASSOCIATION_SYSTEM_ACTOR =
      "urn:li:corpuser:__datahub_system_sibling_hook";
  public static final String DBT_PLATFORM_NAME = "dbt";

  // Older dbt sources produced lowercase subtypes, whereas we now
  // produce titlecase subtypes. We need to handle both cases to
  // maintain backwards compatibility.
  public static final String SOURCE_SUBTYPE_V1 = "source";
  public static final String SOURCE_SUBTYPE_V2 = "Source";

  private final SystemEntityClient systemEntityClient;
  private final EntitySearchService entitySearchService;
  private final boolean isEnabled;
  private OperationContext systemOperationContext;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public SiblingAssociationHook(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final EntitySearchService searchService,
      @Nonnull @Value("${siblings.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${siblings.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.systemEntityClient = systemEntityClient;
    entitySearchService = searchService;
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public SiblingAssociationHook(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final EntitySearchService searchService,
      @Nonnull Boolean isEnabled) {
    this(systemEntityClient, searchService, isEnabled, "");
  }

  @Value("${siblings.enabled:false}")
  private Boolean enabled;

  @VisibleForTesting
  void setEnabled(Boolean newValue) {
    enabled = newValue;
  }

  @Override
  public SiblingAssociationHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    return this;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    if (enabled && isEligibleForProcessing(event)) {

      log.info(
          "Urn {} with aspect {} received by Sibling Hook.",
          event.getEntityUrn(),
          event.getAspectName());

      final Urn urn = getUrnFromEvent(event);

      DatasetUrn datasetUrn = null;
      try {
        datasetUrn = DatasetUrn.createFromUrn(urn);
      } catch (URISyntaxException e) {
        log.error("Error while parsing urn {} : {}", event.getEntityUrn(), e.toString());
        throw new RuntimeException("Failed to parse entity urn, skipping processing.", e);
      }

      // if we are seeing the key, this means the entity may have been deleted and re-ingested
      // in this case we want to re-create its siblings aspects
      if (event.getAspectName().equals(DATASET_KEY_ASPECT_NAME)) {
        handleEntityKeyEvent(datasetUrn);
      } else if (datasetUrn.getPlatformEntity().getPlatformNameEntity().equals(DBT_PLATFORM_NAME)) {
        handleDbtDatasetEvent(event, datasetUrn);
      } else {
        handleSourceDatasetEvent(event, datasetUrn);
      }
    }
  }

  private void handleEntityKeyEvent(DatasetUrn datasetUrn) {
    Filter entitiesWithYouAsSiblingFilter = createFilterForEntitiesWithYouAsSibling(datasetUrn);
    final SearchResult searchResult =
        entitySearchService.search(
            systemOperationContext.withSearchFlags(
                flags ->
                    flags.setFulltext(false).setSkipAggregates(true).setSkipHighlighting(true)),
            List.of(DATASET_ENTITY_NAME),
            "*",
            entitiesWithYouAsSiblingFilter,
            null,
            0,
            10);

    // we have a match of an entity with you as a sibling, associate yourself back
    searchResult
        .getEntities()
        .forEach(
            entity -> {
              if (!entity.getEntity().equals(datasetUrn)) {
                if (datasetUrn
                    .getPlatformEntity()
                    .getPlatformNameEntity()
                    .equals(DBT_PLATFORM_NAME)) {
                  setSiblingsAndSoftDeleteSibling(
                      datasetUrn, searchResult.getEntities().get(0).getEntity());
                } else {
                  setSiblingsAndSoftDeleteSibling(
                      searchResult.getEntities().get(0).getEntity(), datasetUrn);
                }
              }
            });
  }

  // If the upstream is a single source system node & subtype is source, then associate the upstream
  // as your sibling
  private void handleDbtDatasetEvent(MetadataChangeLog event, DatasetUrn datasetUrn) {
    // we need both UpstreamLineage & Subtypes to determine whether to associate
    UpstreamLineage upstreamLineage = null;
    SubTypes subTypesAspectOfEntity = null;

    if (event.getAspectName().equals(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      upstreamLineage = getUpstreamLineageFromEvent(event);
      subTypesAspectOfEntity = getSubtypesFromEntityClient(datasetUrn);
    }

    if (event.getAspectName().equals(SUB_TYPES_ASPECT_NAME)) {
      subTypesAspectOfEntity = getSubtypesFromEvent(event);
      upstreamLineage = getUpstreamLineageFromEntityClient(datasetUrn);
    }

    if (upstreamLineage != null
        && subTypesAspectOfEntity != null
        && upstreamLineage.hasUpstreams()
        && subTypesAspectOfEntity.hasTypeNames()
        && (subTypesAspectOfEntity.getTypeNames().contains(SOURCE_SUBTYPE_V1)
            || subTypesAspectOfEntity.getTypeNames().contains(SOURCE_SUBTYPE_V2))) {
      UpstreamArray upstreams = upstreamLineage.getUpstreams();
      if (upstreams.size() == 1
          && !upstreams
              .get(0)
              .getDataset()
              .getPlatformEntity()
              .getPlatformNameEntity()
              .equals(DBT_PLATFORM_NAME)) {
        setSiblingsAndSoftDeleteSibling(datasetUrn, upstreams.get(0).getDataset());
      }
    }
  }

  // if the dataset is not dbt--- it may be produced by a dbt dataset. If so, associate them as
  // siblings
  private void handleSourceDatasetEvent(MetadataChangeLog event, DatasetUrn sourceUrn) {
    if (event.getAspectName().equals(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      UpstreamLineage upstreamLineage = getUpstreamLineageFromEvent(event);
      if (upstreamLineage != null && upstreamLineage.hasUpstreams()) {
        UpstreamArray upstreams = upstreamLineage.getUpstreams();

        // an entity can have merged lineage (eg. dbt + snowflake), but by default siblings are only
        // between dbt <> non-dbt
        UpstreamArray dbtUpstreams =
            new UpstreamArray(
                upstreams.stream()
                    .filter(
                        obj ->
                            obj.getDataset()
                                .getPlatformEntity()
                                .getPlatformNameEntity()
                                .equals(DBT_PLATFORM_NAME))
                    .collect(Collectors.toList()));
        // We're assuming a data asset (eg. snowflake table) will only ever be downstream of 1 dbt
        // model
        if (dbtUpstreams.size() == 1) {
          setSiblingsAndSoftDeleteSibling(dbtUpstreams.get(0).getDataset(), sourceUrn);
        } else if (dbtUpstreams.size() > 1) {
          log.error(
              "{} has an unexpected number of dbt upstreams: {}. Not adding any as siblings.",
              sourceUrn.toString(),
              dbtUpstreams.size());
        }
      }
    }
  }

  private void setSiblingsAndSoftDeleteSibling(Urn dbtUrn, Urn sourceUrn) {
    Siblings existingDbtSiblingAspect = getSiblingsFromEntityClient(dbtUrn);
    Siblings existingSourceSiblingAspect = getSiblingsFromEntityClient(sourceUrn);

    log.info("Associating {} and {} as siblings.", dbtUrn.toString(), sourceUrn.toString());

    if (existingDbtSiblingAspect != null
        && existingSourceSiblingAspect != null
        && existingDbtSiblingAspect.getSiblings().contains(sourceUrn.toString())
        && existingSourceSiblingAspect.getSiblings().contains(dbtUrn.toString())) {
      // we have already connected them- we can abort here
      return;
    }

    AuditStamp auditStamp = getAuditStamp();

    // set source as a sibling of dbt
    Siblings dbtSiblingAspect = new Siblings();
    dbtSiblingAspect.setSiblings(new UrnArray(ImmutableList.of(sourceUrn)));
    dbtSiblingAspect.setPrimary(true);

    MetadataChangeProposal dbtSiblingProposal = new MetadataChangeProposal();
    GenericAspect dbtSiblingAspectSerialized = GenericRecordUtils.serializeAspect(dbtSiblingAspect);

    dbtSiblingProposal.setAspect(dbtSiblingAspectSerialized);
    dbtSiblingProposal.setAspectName(SIBLINGS_ASPECT_NAME);
    dbtSiblingProposal.setEntityType(DATASET_ENTITY_NAME);
    dbtSiblingProposal.setChangeType(ChangeType.UPSERT);
    dbtSiblingProposal.setEntityUrn(dbtUrn);

    try {
      systemEntityClient.ingestProposal(systemOperationContext, dbtSiblingProposal, true);
    } catch (RemoteInvocationException e) {
      log.error("Error while associating {} with {}: {}", dbtUrn, sourceUrn, e.toString());
      throw new RuntimeException("Error ingesting sibling proposal. Skipping processing.", e);
    }

    // set dbt as a sibling of source

    Siblings sourceSiblingAspect = new Siblings();
    if (existingSourceSiblingAspect != null) {
      sourceSiblingAspect = existingSourceSiblingAspect;
    }

    UrnArray newSiblingsUrnArray =
        sourceSiblingAspect.hasSiblings() ? sourceSiblingAspect.getSiblings() : new UrnArray();
    if (!newSiblingsUrnArray.contains(dbtUrn)) {
      newSiblingsUrnArray.add(dbtUrn);
    }

    // clean up any references to stale siblings that have been deleted
    List<Urn> filteredNewSiblingsArray =
        newSiblingsUrnArray.stream()
            .filter(
                urn -> {
                  try {
                    return systemEntityClient.exists(systemOperationContext, urn);
                  } catch (RemoteInvocationException e) {
                    log.error("Error while checking existence of {}: {}", urn, e.toString());
                    throw new RuntimeException("Error checking existence. Skipping processing.", e);
                  }
                })
            .collect(Collectors.toList());

    sourceSiblingAspect.setSiblings(new UrnArray(filteredNewSiblingsArray));
    sourceSiblingAspect.setPrimary(false);

    MetadataChangeProposal sourceSiblingProposal = new MetadataChangeProposal();
    GenericAspect sourceSiblingAspectSerialized =
        GenericRecordUtils.serializeAspect(sourceSiblingAspect);

    sourceSiblingProposal.setAspect(sourceSiblingAspectSerialized);
    sourceSiblingProposal.setAspectName(SIBLINGS_ASPECT_NAME);
    sourceSiblingProposal.setEntityType(DATASET_ENTITY_NAME);
    sourceSiblingProposal.setChangeType(ChangeType.UPSERT);
    sourceSiblingProposal.setEntityUrn(sourceUrn);

    try {
      systemEntityClient.ingestProposal(systemOperationContext, sourceSiblingProposal, true);
    } catch (RemoteInvocationException e) {
      log.error("Error while associating {} with {}: {}", dbtUrn, sourceUrn, e.toString());
      throw new RuntimeException("Error ingesting sibling proposal. Skipping processing.", e);
    }
  }

  /**
   * Returns true if the event should be processed, which is only true if the event represents a
   * dataset for now
   */
  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    return event.getEntityType().equals("dataset")
        && !event.getChangeType().equals(ChangeType.DELETE)
        && (event.getAspectName().equals(UPSTREAM_LINEAGE_ASPECT_NAME)
            || event.getAspectName().equals(SUB_TYPES_ASPECT_NAME)
            || event.getAspectName().equals(DATASET_KEY_ASPECT_NAME));
  }

  /**
   * Extracts and returns an {@link Urn} from a {@link MetadataChangeLog}. Extracts from either an
   * entityUrn or entityKey field, depending on which is present.
   */
  private Urn getUrnFromEvent(final MetadataChangeLog event) {
    EntitySpec entitySpec;
    try {
      entitySpec = systemOperationContext.getEntityRegistry().getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException(
          "Failed to get urn from MetadataChangeLog event. Skipping processing.", e);
    }
    // Extract an URN from the Log Event.
    return EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec());
  }

  /**
   * Deserializes and returns an instance of {@link UpstreamLineage} extracted from a {@link
   * MetadataChangeLog} event.
   */
  private UpstreamLineage getUpstreamLineageFromEvent(final MetadataChangeLog event) {
    EntitySpec entitySpec;
    if (!event.getAspectName().equals(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      return null;
    }

    try {
      entitySpec = systemOperationContext.getEntityRegistry().getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException(
          "Failed to get UpstreamLineage from MetadataChangeLog event. Skipping processing.", e);
    }
    return (UpstreamLineage)
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            entitySpec.getAspectSpec(UPSTREAM_LINEAGE_ASPECT_NAME));
  }

  /**
   * Deserializes and returns an instance of {@link SubTypes} extracted from a {@link
   * MetadataChangeLog} event.
   */
  private SubTypes getSubtypesFromEvent(final MetadataChangeLog event) {
    EntitySpec entitySpec;
    if (!event.getAspectName().equals(SUB_TYPES_ASPECT_NAME)) {
      return null;
    }

    try {
      entitySpec = systemOperationContext.getEntityRegistry().getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException(
          "Failed to get SubTypes from MetadataChangeLog event. Skipping processing.", e);
    }
    return (SubTypes)
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            entitySpec.getAspectSpec(SUB_TYPES_ASPECT_NAME));
  }

  @SneakyThrows
  private AuditStamp getAuditStamp() {
    return new AuditStamp()
        .setActor(Urn.createFromString(SIBLING_ASSOCIATION_SYSTEM_ACTOR))
        .setTime(System.currentTimeMillis());
  }

  private Filter createFilterForEntitiesWithYouAsSibling(final Urn entityUrn) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();

    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    final Criterion urnCriterion =
        buildCriterion("siblings.keyword", Condition.EQUAL, entityUrn.toString());

    andCriterion.add(urnCriterion);

    conjunction.setAnd(andCriterion);

    disjunction.add(conjunction);

    filter.setOr(disjunction);
    return filter;
  }

  private SubTypes getSubtypesFromEntityClient(final Urn urn) {
    try {
      EntityResponse entityResponse =
          systemEntityClient.getV2(
              systemOperationContext, urn, ImmutableSet.of(SUB_TYPES_ASPECT_NAME));

      if (entityResponse != null
          && entityResponse.hasAspects()
          && entityResponse.getAspects().containsKey(Constants.SUB_TYPES_ASPECT_NAME)) {
        return new SubTypes(
            entityResponse.getAspects().get(Constants.SUB_TYPES_ASPECT_NAME).getValue().data());
      } else {
        return null;
      }
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException("Failed to retrieve Subtypes", e);
    }
  }

  private UpstreamLineage getUpstreamLineageFromEntityClient(final Urn urn) {
    try {
      EntityResponse entityResponse =
          systemEntityClient.getV2(
              systemOperationContext, urn, ImmutableSet.of(UPSTREAM_LINEAGE_ASPECT_NAME));

      if (entityResponse != null
          && entityResponse.hasAspects()
          && entityResponse.getAspects().containsKey(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)) {
        return new UpstreamLineage(
            entityResponse
                .getAspects()
                .get(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)
                .getValue()
                .data());
      } else {
        return null;
      }
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException("Failed to retrieve UpstreamLineage", e);
    }
  }

  private Siblings getSiblingsFromEntityClient(final Urn urn) {
    try {
      EntityResponse entityResponse =
          systemEntityClient.getV2(
              systemOperationContext, urn, ImmutableSet.of(SIBLINGS_ASPECT_NAME));

      if (entityResponse != null
          && entityResponse.hasAspects()
          && entityResponse.getAspects().containsKey(Constants.SIBLINGS_ASPECT_NAME)) {
        return new Siblings(
            entityResponse.getAspects().get(Constants.SIBLINGS_ASPECT_NAME).getValue().data());
      } else {
        return null;
      }
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException("Failed to retrieve UpstreamLineage", e);
    }
  }
}
