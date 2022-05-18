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
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.SearchServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  public static final String DBT_PLATFORM_NAME = "dbt";

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

      // get entity's name--- the urns may not be exact matches
      DatasetProperties existingEntityProperties =
          (DatasetProperties) _entityService.getLatestAspect(datasetUrn, DATASET_PROPERTIES_ASPECT_NAME);

      String datasetNameToSearchFor = getDatasetNameToSearchFor(datasetUrn, existingEntityProperties, event);

      if (datasetUrn.getPlatformEntity().getPlatformNameEntity().equals(DBT_PLATFORM_NAME)) {
        handleDbtDatasetEvent(datasetNameToSearchFor, datasetUrn);
      } else {
        handleSourceDatasetEvent(datasetNameToSearchFor, datasetUrn);
      }
    }
  }

  // If the dataset is dbt--- look for the source entity of this dbt element.
  // If none can be found, look for another matching dbt element who already may have soft
  // deleted the source entity and linked to it via Siblings aspect
  private void handleDbtDatasetEvent(String datasetNameToSearchFor, DatasetUrn datasetUrn) {
    Siblings siblingAspectOfEntity =
        (Siblings) _entityService.getLatestAspect(
            datasetUrn,
            SIBLINGS_ASPECT_NAME
        );


    // if the dbt node already have a sibling entity, we don't need to search for one.
    if (siblingAspectOfEntity != null) {
      return;
    }

    Filter filterForFindingSiblingEntity =
        createFilter(datasetNameToSearchFor, datasetUrn.getOriginEntity(), null);

    final SearchResult searchResult = _searchService.search(
        "dataset",
        "*",
        filterForFindingSiblingEntity,
        null,
        0,
        10,
        null);


    if (searchResult.getEntities().size() > 0) {
      // our matches may either be source nodes or dbt.
      // if there is a source node - associate it
      // if there are just dbt nodes -- take their sibling aspect
      Stream<SearchEntity> sourceResultsStream = searchResult.getEntities()
          .stream()
          .filter(entity -> {
            try {
              return !(DatasetUrn.createFromUrn(entity.getEntity())).getPlatformEntity()
                  .getPlatformNameEntity()
                  .equals(DBT_PLATFORM_NAME);
            } catch (URISyntaxException e) {
              e.printStackTrace();
              return false;
            }
          });

      List<SearchEntity> sourceResults = sourceResultsStream.collect(Collectors.toList());

      if (sourceResults.size() > 0) {
        // associate yourself as a sibling of the source node
        sourceResults.forEach(entity -> {
          setSiblingsAndSoftDeleteSibling(datasetUrn, entity.getEntity());
        });
      } else {
        // yank the sibling of aspect of another matching dbt node (for example, this may be a source and you
        // may be the incremental representing the same underlying dataset)
        Siblings siblingAspectOfCorrespondingDbtNode =
            (Siblings) _entityService.getLatestAspect(
                searchResult.getEntities().get(0).getEntity(),
                SIBLINGS_ASPECT_NAME
            );
        if (siblingAspectOfCorrespondingDbtNode != null) {
          setSiblingsAndSoftDeleteSibling(datasetUrn, siblingAspectOfCorrespondingDbtNode.getSiblings().get(0));
        }
      }
    }
  }

  // if the dataset is not dbt--- it may be a backing dataset. look for the dbt partner(s).
  private void handleSourceDatasetEvent(String datasetNameToSearchFor, DatasetUrn datasetUrn) {
    Filter filterForFindingSiblingEntity = createFilter(
        datasetNameToSearchFor,
        datasetUrn.getOriginEntity(),
        DataPlatformUrn.createFromTuple("dataPlatform", DBT_PLATFORM_NAME)
    );

    final SearchResult searchResult = _searchService.search(
        "dataset",
        "*",
        filterForFindingSiblingEntity,
        null,
        0,
        10,
        null);

    // we have a match of some dbt entities, become their siblings
    searchResult.getEntities().forEach(entity -> {
      setSiblingsAndSoftDeleteSibling(entity.getEntity(), datasetUrn);
    });
  }

  private void setSiblingsAndSoftDeleteSibling(Urn dbtUrn, Urn sourceUrn) {
    Siblings existingDbtSiblingAspect =
        (Siblings) _entityService.getLatestAspect(dbtUrn, SIBLINGS_ASPECT_NAME);
    Siblings existingSourceSiblingAspect =
        (Siblings) _entityService.getLatestAspect(sourceUrn, SIBLINGS_ASPECT_NAME);
    Status existingSourceStatusAspect =
        (Status) _entityService.getLatestAspect(sourceUrn, STATUS_ASPECT_NAME);

    log.info("Associating {} and {} as siblings.", dbtUrn.toString(), sourceUrn.toString());

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
    if (existingSourceSiblingAspect != null) {
      sourceSiblingAspect = existingSourceSiblingAspect;
    }

    UrnArray newSiblingsUrnArray =
        sourceSiblingAspect.hasSiblings() ? sourceSiblingAspect.getSiblings() : new UrnArray();
    if (!newSiblingsUrnArray.contains(dbtUrn)) {
      newSiblingsUrnArray.add(dbtUrn);
    }

    sourceSiblingAspect.setSiblings(newSiblingsUrnArray);
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
    return event.getEntityType().equals("dataset")
        && !event.getChangeType().equals(ChangeType.DELETE)
        && (
            event.getAspectName().equals("status")
                || event.getAspectName().equals("datasetKey")
                || event.getAspectName().equals("datasetProperties")
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

  private Filter createFilter(
      final @Nullable String name,
      final @Nullable FabricType origin,
      final @Nullable Urn platformUrn
      ) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();

    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    if (name != null) {
      final Criterion nameCriterion = new Criterion();
      nameCriterion.setField("name.keyword");
      nameCriterion.setValue(name);
      nameCriterion.setCondition(Condition.EQUAL);
      andCriterion.add(nameCriterion);
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

  /**
   * Deserializes and returns an instance of {@link DatasetProperties} extracted from a {@link MetadataChangeLog} event.
   */
  private DatasetProperties getPropertiesFromEvent(final MetadataChangeLog event) {
    EntitySpec entitySpec;
    if (!event.getAspectName().equals(DATASET_PROPERTIES_ASPECT_NAME)) {
      return null;
    }

    try {
      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      throw new RuntimeException("Failed to get DatasetProperties from MetadataChangeLog event. Skipping processing.", e);
    }
    return (DatasetProperties) GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(),
        event.getAspect().getContentType(),
        entitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME));
  }

  private String getDatasetNameToSearchFor(
      DatasetUrn datasetUrn,
      DatasetProperties existingEntityProperties,
      MetadataChangeLog event
  ) {
    DatasetProperties propertiesFromEvent = getPropertiesFromEvent(event);
    if (propertiesFromEvent != null && propertiesFromEvent.hasName()) {
      return propertiesFromEvent.getName();
    }

    if (existingEntityProperties != null && existingEntityProperties.hasName()) {
      return existingEntityProperties.getName();
    }

    return datasetUrn.getDatasetNameEntity();
  }

}
