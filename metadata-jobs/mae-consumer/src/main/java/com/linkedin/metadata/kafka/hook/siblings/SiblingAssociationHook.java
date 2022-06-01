package com.linkedin.metadata.kafka.hook.siblings;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Siblings;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.SearchServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import static com.linkedin.metadata.Constants.*;


/**
 * This hook associates dbt datasets with their sibling entities
 */
@Slf4j
@Component
@Singleton
@Import({EntityRegistryFactory.class, EntityServiceFactory.class, SearchServiceFactory.class})
public class SiblingAssociationHook implements MetadataChangeLogHook {

  public static final String SIBLING_ASSOCIATION_SYSTEM_ACTOR = "urn:li:corpuser:__datahub_system_sibling_hook";
  public static final String DBT_PLATFORM_NAME = "dbt";
  public static final String SOURCE_SUBTYPE = "source";

  private final EntityRegistry _entityRegistry;
  private final EntityService _entityService;
  private final SearchService _searchService;

  @Autowired
  public SiblingAssociationHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final EntityService entityService,
      @Nonnull final SearchService searchService
  ) {
    _entityRegistry = entityRegistry;
    _entityService = entityService;
    _searchService = searchService;
  }

  @Value("${siblings.enabled:false}")
  private Boolean enabled;

  @VisibleForTesting
  void setEnabled(Boolean newValue) {
    enabled = newValue;
  }

  @Override
  public void init() {
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    if (enabled && isEligibleForProcessing(event)) {

      log.info("Urn {} received by Sibling Hook.", event.getEntityUrn());

      final Urn urn = getUrnFromEvent(event);

      DatasetUrn datasetUrn = null;
      try {
        datasetUrn = DatasetUrn.createFromUrn(urn);
      } catch (URISyntaxException e) {
        log.error("Error while parsing urn {} : {}", event.getEntityUrn(), e.toString());
        throw new RuntimeException("Failed to parse entity urn, skipping processing.", e);
      }

      if (datasetUrn.getPlatformEntity().getPlatformNameEntity().equals(DBT_PLATFORM_NAME)) {
        handleDbtDatasetEvent(event, datasetUrn);
      } else {
        handleSourceDatasetEvent(event, datasetUrn);
      }
    }
  }

  // If th upstream is a single source system node & subtype is source, then associate the upstream as your sibling
  private void handleDbtDatasetEvent(MetadataChangeLog event, DatasetUrn datasetUrn) {
    // we need both UpstreamLineage & Subtypes to determine whether to associate
    UpstreamLineage upstreamLineage = null;
    SubTypes subTypesAspectOfEntity = null;

    if (event.getAspectName().equals(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      upstreamLineage = getUpstreamLineageFromEvent(event);
      subTypesAspectOfEntity =
          (SubTypes) _entityService.getLatestAspect(
              datasetUrn,
              SUB_TYPES_ASPECT_NAME
          );

    }

    if (event.getAspectName().equals(SUB_TYPES_ASPECT_NAME)) {
      subTypesAspectOfEntity = getSubtypesFromEvent(event);
      upstreamLineage =
          (UpstreamLineage) _entityService.getLatestAspect(
              datasetUrn,
              UPSTREAM_LINEAGE_ASPECT_NAME
          );
    }

    if (
        upstreamLineage != null
            && subTypesAspectOfEntity != null
            && upstreamLineage.hasUpstreams()
            && subTypesAspectOfEntity.hasTypeNames()
            && subTypesAspectOfEntity.getTypeNames().contains(SOURCE_SUBTYPE)
    ) {
      UpstreamArray upstreams = upstreamLineage.getUpstreams();
      if (
          upstreams.size() == 1
              && !upstreams.get(0).getDataset().getPlatformEntity().getPlatformNameEntity().equals(DBT_PLATFORM_NAME)) {
        setSiblingsAndSoftDeleteSibling(datasetUrn, upstreams.get(0).getDataset());
      }
    }
  }

  // if the dataset is not dbt--- it may be produced by a dbt dataset. If so, associate them as siblings
  private void handleSourceDatasetEvent(MetadataChangeLog event, DatasetUrn sourceUrn) {

    // if the source entity is a sibling we need to make sure it stays soft deleted
//    if (event.getAspectName().equals(STATUS_ASPECT_NAME)) {
//      Status eventStatusAspect = getStatusFromEvent(event);
//      if (!eventStatusAspect.isRemoved()) {
//        Siblings existingSourceSiblingAspect =
//            (Siblings) _entityService.getLatestAspect(sourceUrn, SIBLINGS_ASPECT_NAME);
//
//        if (
//            existingSourceSiblingAspect != null
//                && existingSourceSiblingAspect.hasSiblings()
//                && existingSourceSiblingAspect.getSiblings().size() > 0) {
//          softDeleteEntity(sourceUrn);
//        }
//      }
//    }

    if (event.getAspectName().equals(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      UpstreamLineage upstreamLineage = getUpstreamLineageFromEvent(event);
      if (upstreamLineage != null && upstreamLineage.hasUpstreams()) {
        UpstreamArray upstreams = upstreamLineage.getUpstreams();
        if (
            upstreams.size() == 1
                && upstreams.get(0).getDataset().getPlatformEntity().getPlatformNameEntity().equals(DBT_PLATFORM_NAME)) {
          setSiblingsAndSoftDeleteSibling(upstreams.get(0).getDataset(), sourceUrn);
        }
      }
    }
  }

  private void setSiblingsAndSoftDeleteSibling(Urn dbtUrn, Urn sourceUrn) {
    Siblings existingDbtSiblingAspect =
        (Siblings) _entityService.getLatestAspect(dbtUrn, SIBLINGS_ASPECT_NAME);
    Siblings existingSourceSiblingAspect =
        (Siblings) _entityService.getLatestAspect(sourceUrn, SIBLINGS_ASPECT_NAME);
//    Status existingSourceStatusAspect =
//        (Status) _entityService.getLatestAspect(sourceUrn, STATUS_ASPECT_NAME);

    log.info("Associating {} and {} as siblings.", dbtUrn.toString(), sourceUrn.toString());

    if (
        existingDbtSiblingAspect != null
            && existingSourceSiblingAspect != null
//            && existingSourceStatusAspect != null
            && existingDbtSiblingAspect.getSiblings().contains(sourceUrn.toString())
            && existingDbtSiblingAspect.getSiblings().contains(dbtUrn.toString())
//            && existingSourceStatusAspect.isRemoved()
    ) {
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

    _entityService.ingestProposal(dbtSiblingProposal, auditStamp);

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
        newSiblingsUrnArray.stream().filter(urn -> _entityService.exists(urn)).collect(Collectors.toList());

    sourceSiblingAspect.setSiblings(new UrnArray(filteredNewSiblingsArray));
    sourceSiblingAspect.setPrimary(false);

    MetadataChangeProposal sourceSiblingProposal = new MetadataChangeProposal();
    GenericAspect sourceSiblingAspectSerialized = GenericRecordUtils.serializeAspect(sourceSiblingAspect);

    sourceSiblingProposal.setAspect(sourceSiblingAspectSerialized);
    sourceSiblingProposal.setAspectName(SIBLINGS_ASPECT_NAME);
    sourceSiblingProposal.setEntityType(DATASET_ENTITY_NAME);
    sourceSiblingProposal.setChangeType(ChangeType.UPSERT);
    sourceSiblingProposal.setEntityUrn(sourceUrn);

    _entityService.ingestProposal(sourceSiblingProposal, auditStamp);
//
//    // soft delete the sibling
//    softDeleteEntity(sourceUrn);
  }

  /**
   * Returns true if the event should be processed, which is only true if the event represents a dataset for now
   */
  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    return event.getEntityType().equals("dataset")
        && !event.getChangeType().equals(ChangeType.DELETE)
        && (
            event.getAspectName().equals("upstreamLineage")
                || event.getAspectName().equals("subType")
//                || event.getAspectName().equals("status")
          );
  }

  /**
   * Extracts and returns an {@link Urn} from a {@link MetadataChangeLog}. Extracts from either an entityUrn
   * or entityKey field, depending on which is present.
   */
  private Urn getUrnFromEvent(final MetadataChangeLog event) {
    EntitySpec entitySpec;
    try {
      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException("Failed to get urn from MetadataChangeLog event. Skipping processing.", e);
    }
    // Extract an URN from the Log Event.
    return EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec());
  }

  /**
   * Deserializes and returns an instance of {@link UpstreamLineage} extracted from a {@link MetadataChangeLog} event.
   */
  private UpstreamLineage getUpstreamLineageFromEvent(final MetadataChangeLog event) {
    EntitySpec entitySpec;
    if (!event.getAspectName().equals(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      return null;
    }

    try {
      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException("Failed to get UpstreamLineage from MetadataChangeLog event. Skipping processing.", e);
    }
    return (UpstreamLineage) GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(),
        event.getAspect().getContentType(),
        entitySpec.getAspectSpec(UPSTREAM_LINEAGE_ASPECT_NAME));
  }

  /**
   * Deserializes and returns an instance of {@link SubTypes} extracted from a {@link MetadataChangeLog} event.
   */
  private SubTypes getSubtypesFromEvent(final MetadataChangeLog event) {
    EntitySpec entitySpec;
    if (!event.getAspectName().equals(SUB_TYPES_ASPECT_NAME)) {
      return null;
    }

    try {
      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException("Failed to get SubTypes from MetadataChangeLog event. Skipping processing.", e);
    }
    return (SubTypes) GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(),
        event.getAspect().getContentType(),
        entitySpec.getAspectSpec(SUB_TYPES_ASPECT_NAME));
  }

//  /**
//   * Deserializes and returns an instance of {@link Status} extracted from a {@link MetadataChangeLog} event.
//   */
//  private Status getStatusFromEvent(final MetadataChangeLog event) {
//    EntitySpec entitySpec;
//    if (!event.getAspectName().equals(STATUS_ASPECT_NAME)) {
//      return null;
//    }
//
//    try {
//      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
//    } catch (IllegalArgumentException e) {
//      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
//      throw new RuntimeException("Failed to get Status from MetadataChangeLog event. Skipping processing.", e);
//    }
//    return (Status) GenericRecordUtils.deserializeAspect(
//        event.getAspect().getValue(),
//        event.getAspect().getContentType(),
//        entitySpec.getAspectSpec(STATUS_ASPECT_NAME));
//  }

  @SneakyThrows
  private AuditStamp getAuditStamp() {
    AuditStamp auditStamp = null;
    return new AuditStamp().setActor(Urn.createFromString(SIBLING_ASSOCIATION_SYSTEM_ACTOR)).setTime(System.currentTimeMillis());
  }

//  private void softDeleteEntity(Urn sourceUrn) {
//    // soft delete the sibling
//    Status sourceStatusAspect = new Status();
//    sourceStatusAspect.setRemoved(true);
//
//    MetadataChangeProposal sourceStatusProposal = new MetadataChangeProposal();
//    GenericAspect sourceStatusSerialized = GenericRecordUtils.serializeAspect(sourceStatusAspect);
//
//    sourceStatusProposal.setAspect(sourceStatusSerialized);
//    sourceStatusProposal.setAspectName(STATUS_ASPECT_NAME);
//    sourceStatusProposal.setEntityType(DATASET_ENTITY_NAME);
//    sourceStatusProposal.setChangeType(ChangeType.UPSERT);
//    sourceStatusProposal.setEntityUrn(sourceUrn);
//
//   _entityService.ingestProposal(sourceStatusProposal, getAuditStamp());
//  }

}
