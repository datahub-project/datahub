package com.linkedin.metadata.kafka.hook.siblings;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.Siblings;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.SearchServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;

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

      log.info("Received {} to search for siblings.");

      final Urn urn = getUrnFromEvent(event);

      if (!event.getChangeType().equals(ChangeType.DELETE)) {
        DatasetUrn datasetUrn = null;
        try {
          datasetUrn = DatasetUrn.createFromUrn(urn);
        } catch (URISyntaxException e) {
          e.printStackTrace();
        }

        // if the dataset is dbt--- look for a non-dbt partner
        if (datasetUrn.getPlatformEntity().getPlatformNameEntity().equals("dbt")) {
          Filter filterForFindingSiblingEntity =
              createFilter(datasetUrn.getDatasetNameEntity(), datasetUrn.getOriginEntity(), null);

          final SearchResult searchResult = _searchService.search(
              "dataset",
              "*",
              filterForFindingSiblingEntity,
              null,
              0,
              10,
              null);

          // we have a match of a base entity, that means we have not soft deleted the entity yet
          // In other words, this is the first time we are seeing the dbt entity. So we must create
          // the sibling connection
          if (searchResult.getEntities().size() > 0) {
            searchResult.getEntities().forEach(entity -> {
                  if (!entity.getEntity().equals(urn)) {
                    setSiblingsAndSoftDeleteSibling(urn, searchResult.getEntities().get(0).getEntity());
                  }
                });
          }
        } else {
        // if the dataset is not dbt--- look for a dbt partner.

          Filter filterForFindingSiblingEntity = createFilter(
              datasetUrn.getDatasetNameEntity(),
              datasetUrn.getOriginEntity(),
              DataPlatformUrn.createFromTuple("dataPlatform", "dbt")
          );

          final SearchResult searchResult = _searchService.search(
              "dataset",
              "*",
              filterForFindingSiblingEntity,
              null,
              0,
              1,
              null);

          // we have a match of a dbt entity, that means we have a sibling.
          if (searchResult.getEntities().size() > 0) {
            setSiblingsAndSoftDeleteSibling(searchResult.getEntities().get(0).getEntity(), urn);
          }
        }
      }
    }
  }

  private void setSiblingsAndSoftDeleteSibling(Urn dbtUrn, Urn sourceUrn) {
    RecordTemplate existingDbtSiblingAspect =
        _entityService.getLatestAspect(dbtUrn, SIBLINGS_ASPECT_NAME);
    RecordTemplate existingSourceSiblingAspect =
        _entityService.getLatestAspect(sourceUrn, SIBLINGS_ASPECT_NAME);
    Status existingSourceStatusAspect =
        (Status) _entityService.getLatestAspect(sourceUrn, STATUS_ASPECT_NAME);

    if (
        existingDbtSiblingAspect != null
            && existingSourceSiblingAspect != null
            && existingSourceStatusAspect != null
            && existingSourceStatusAspect.isRemoved()
    ) {
      // we have already connected them- we can abort here
      return;
    }

    AuditStamp auditStamp = null;
    try {
      auditStamp =
          new AuditStamp().setActor(Urn.createFromString(SIBLING_ASSOCIATION_SYSTEM_ACTOR)).setTime(System.currentTimeMillis());
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

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
    sourceSiblingAspect.setSiblings(new UrnArray(ImmutableList.of(dbtUrn)));
    sourceSiblingAspect.setPrimary(false);

    MetadataChangeProposal sourceSiblingProposal = new MetadataChangeProposal();
    GenericAspect sourceSiblingAspectSerialized = GenericRecordUtils.serializeAspect(sourceSiblingAspect);

    sourceSiblingProposal.setAspect(sourceSiblingAspectSerialized);
    sourceSiblingProposal.setAspectName(SIBLINGS_ASPECT_NAME);
    sourceSiblingProposal.setEntityType(DATASET_ENTITY_NAME);
    sourceSiblingProposal.setChangeType(ChangeType.UPSERT);
    sourceSiblingProposal.setEntityUrn(sourceUrn);

    _entityService.ingestProposal(sourceSiblingProposal, auditStamp);

    // soft delete the sibling
    Status sourceStatusAspect = new Status();
    sourceStatusAspect.setRemoved(true);

    MetadataChangeProposal sourceStatusProposal = new MetadataChangeProposal();
    GenericAspect sourceStatusSerialized = GenericRecordUtils.serializeAspect(sourceStatusAspect);

    sourceStatusProposal.setAspect(sourceStatusSerialized);
    sourceStatusProposal.setAspectName(STATUS_ASPECT_NAME);
    sourceStatusProposal.setEntityType(DATASET_ENTITY_NAME);
    sourceStatusProposal.setChangeType(ChangeType.UPSERT);
    sourceStatusProposal.setEntityUrn(sourceUrn);

    _entityService.ingestProposal(sourceStatusProposal, auditStamp);
  }

  /**
   * Returns true if the event should be processed, which is only true if the event represents a dataset for now
   */
  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    return event.getEntityType().equals("dataset");
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

  private Filter createFilter(
      final @Nullable String id,
      final @Nullable FabricType origin,
      final @Nullable Urn platformUrn
      ) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();

    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    if (id != null) {
      final Criterion idCriterion = new Criterion();
      idCriterion.setField("id.keyword");
      idCriterion.setValue(id);
      idCriterion.setCondition(Condition.EQUAL);
      andCriterion.add(idCriterion);
    }

    if (origin != null) {
      final Criterion originCriterion = new Criterion();
      originCriterion.setField("origin.keyword");
      originCriterion.setValue(origin.toString());
      originCriterion.setCondition(Condition.EQUAL);
      andCriterion.add(originCriterion);
    }

    if (platformUrn != null) {
      final Criterion platformCriterion = new Criterion();
      platformCriterion.setField("platform.keyword");
      platformCriterion.setValue(platformUrn.toString());
      platformCriterion.setCondition(Condition.EQUAL);
      andCriterion.add(platformCriterion);
    }

    conjunction.setAnd(andCriterion);

    disjunction.add(conjunction);

    filter.setOr(disjunction);
    return filter;
  }

}
