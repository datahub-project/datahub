package com.linkedin.metadata.timeline;

import static com.linkedin.common.urn.VersionedUrnUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.eventgenerator.ApplicationsChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.DataProductPropertiesChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.DatasetPropertiesChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.DocumentInfoChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.DomainPropertiesChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.EditableDatasetPropertiesChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.EditableSchemaMetadataChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorFactory;
import com.linkedin.metadata.timeline.eventgenerator.GlobalTagsChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.GlossaryRelatedTermsChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.GlossaryTermInfoChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.GlossaryTermsChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.InstitutionalMemoryChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.OwnershipChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.SchemaMetadataChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.SingleDomainChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.StructuredPropertyChangeEventGenerator;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import jakarta.json.JsonValue;
import java.io.StringReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
public class TimelineServiceImpl implements TimelineService {

  private static final long DEFAULT_LOOKBACK_TIME_WINDOW_MILLIS =
      7 * 24 * 60 * 60 * 1000L; // 1 week lookback
  private static final int ASPECT_ROW_MULTIPLIER = 10;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  private static final long FIRST_TRANSACTION_ID = 0;
  private static final String BUILD_VALUE_COMPUTED = "computed";

  private final AspectDao _aspectDao;
  private final EntityChangeEventGeneratorFactory _entityChangeEventGeneratorFactory;
  private final EntityRegistry _entityRegistry;
  private final HashMap<String, HashMap<ChangeCategory, Set<String>>>
      entityTypeElementAspectRegistry = new HashMap<>();

  public TimelineServiceImpl(@Nonnull AspectDao aspectDao, @Nonnull EntityRegistry entityRegistry) {
    this._aspectDao = aspectDao;
    _entityRegistry = entityRegistry;

    // TODO: Simplify this structure.
    // TODO: Load up from yaml file
    // Dataset registry
    HashMap<ChangeCategory, Set<String>> datasetElementAspectRegistry = new HashMap<>();
    _entityChangeEventGeneratorFactory = new EntityChangeEventGeneratorFactory();
    String entityType = DATASET_ENTITY_NAME;
    for (ChangeCategory elementName : ChangeCategory.values()) {
      Set<String> aspects = new HashSet<>();
      switch (elementName) {
        case TAG:
          {
            aspects.add(SCHEMA_METADATA_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                SCHEMA_METADATA_ASPECT_NAME,
                new SchemaMetadataChangeEventGenerator());
            aspects.add(EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                new EditableSchemaMetadataChangeEventGenerator());
            aspects.add(GLOBAL_TAGS_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                GLOBAL_TAGS_ASPECT_NAME,
                new GlobalTagsChangeEventGenerator());
          }
          break;
        case OWNERSHIP:
          {
            aspects.add(OWNERSHIP_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                OWNERSHIP_ASPECT_NAME,
                new OwnershipChangeEventGenerator());
          }
          break;
        case DOCUMENTATION:
          {
            aspects.add(INSTITUTIONAL_MEMORY_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                INSTITUTIONAL_MEMORY_ASPECT_NAME,
                new InstitutionalMemoryChangeEventGenerator());
            aspects.add(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
                new EditableDatasetPropertiesChangeEventGenerator());
            aspects.add(DATASET_PROPERTIES_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                DATASET_PROPERTIES_ASPECT_NAME,
                new DatasetPropertiesChangeEventGenerator());
            aspects.add(EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                new EditableSchemaMetadataChangeEventGenerator());
            aspects.add(SCHEMA_METADATA_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                SCHEMA_METADATA_ASPECT_NAME,
                new SchemaMetadataChangeEventGenerator());
          }
          break;
        case GLOSSARY_TERM:
          {
            aspects.add(GLOSSARY_TERMS_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                GLOSSARY_TERMS_ASPECT_NAME,
                new GlossaryTermsChangeEventGenerator());
            aspects.add(EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                new EditableSchemaMetadataChangeEventGenerator());
          }
          break;
        case TECHNICAL_SCHEMA:
          {
            aspects.add(SCHEMA_METADATA_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                SCHEMA_METADATA_ASPECT_NAME,
                new SchemaMetadataChangeEventGenerator());
          }
          break;
        case DOMAIN:
          {
            aspects.add(DOMAINS_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                DOMAINS_ASPECT_NAME,
                new SingleDomainChangeEventGenerator());
          }
          break;
        case STRUCTURED_PROPERTY:
          {
            aspects.add(STRUCTURED_PROPERTIES_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                STRUCTURED_PROPERTIES_ASPECT_NAME,
                new StructuredPropertyChangeEventGenerator());
          }
          break;
        case APPLICATION:
          {
            aspects.add(APPLICATION_MEMBERSHIP_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityType,
                elementName,
                APPLICATION_MEMBERSHIP_ASPECT_NAME,
                new ApplicationsChangeEventGenerator());
          }
          break;
        default:
          break;
      }
      datasetElementAspectRegistry.put(elementName, aspects);
    }

    // GlossaryTerm registry
    HashMap<ChangeCategory, Set<String>> glossaryTermElementAspectRegistry = new HashMap<>();
    String entityTypeGlossaryTerm = GLOSSARY_TERM_ENTITY_NAME;
    for (ChangeCategory elementName : ChangeCategory.values()) {
      Set<String> aspects = new HashSet<>();
      switch (elementName) {
        case OWNERSHIP:
          {
            aspects.add(OWNERSHIP_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeGlossaryTerm,
                elementName,
                OWNERSHIP_ASPECT_NAME,
                new OwnershipChangeEventGenerator());
          }
          break;
        case DOCUMENTATION:
          {
            aspects.add(GLOSSARY_TERM_INFO_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeGlossaryTerm,
                elementName,
                GLOSSARY_TERM_INFO_ASPECT_NAME,
                new GlossaryTermInfoChangeEventGenerator());
          }
          break;
        case GLOSSARY_TERM:
          {
            aspects.add(GLOSSARY_RELATED_TERM_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeGlossaryTerm,
                elementName,
                GLOSSARY_RELATED_TERM_ASPECT_NAME,
                new GlossaryRelatedTermsChangeEventGenerator());
          }
          break;
        case DOMAIN:
          {
            aspects.add(DOMAINS_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeGlossaryTerm,
                elementName,
                DOMAINS_ASPECT_NAME,
                new SingleDomainChangeEventGenerator());
          }
          break;
        case STRUCTURED_PROPERTY:
          {
            aspects.add(STRUCTURED_PROPERTIES_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeGlossaryTerm,
                elementName,
                STRUCTURED_PROPERTIES_ASPECT_NAME,
                new StructuredPropertyChangeEventGenerator());
          }
          break;
        case APPLICATION:
          {
            aspects.add(APPLICATION_MEMBERSHIP_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeGlossaryTerm,
                elementName,
                APPLICATION_MEMBERSHIP_ASPECT_NAME,
                new ApplicationsChangeEventGenerator());
          }
          break;
        default:
          break;
      }
      glossaryTermElementAspectRegistry.put(elementName, aspects);
    }

    // Domain registry
    HashMap<ChangeCategory, Set<String>> domainElementAspectRegistry = new HashMap<>();
    String entityTypeDomain = DOMAIN_ENTITY_NAME;
    for (ChangeCategory elementName : ChangeCategory.values()) {
      Set<String> aspects = new HashSet<>();
      switch (elementName) {
        case OWNERSHIP:
          {
            aspects.add(OWNERSHIP_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDomain,
                elementName,
                OWNERSHIP_ASPECT_NAME,
                new OwnershipChangeEventGenerator());
          }
          break;
        case DOCUMENTATION:
          {
            aspects.add(INSTITUTIONAL_MEMORY_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDomain,
                elementName,
                INSTITUTIONAL_MEMORY_ASPECT_NAME,
                new InstitutionalMemoryChangeEventGenerator());
            aspects.add(DOMAIN_PROPERTIES_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDomain,
                elementName,
                DOMAIN_PROPERTIES_ASPECT_NAME,
                new DomainPropertiesChangeEventGenerator());
          }
          break;
        case STRUCTURED_PROPERTY:
          {
            aspects.add(STRUCTURED_PROPERTIES_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDomain,
                elementName,
                STRUCTURED_PROPERTIES_ASPECT_NAME,
                new StructuredPropertyChangeEventGenerator());
          }
          break;
        default:
          break;
      }
      domainElementAspectRegistry.put(elementName, aspects);
    }

    // DataProduct registry
    HashMap<ChangeCategory, Set<String>> dataProductElementAspectRegistry = new HashMap<>();
    String entityTypeDataProduct = DATA_PRODUCT_ENTITY_NAME;
    for (ChangeCategory elementName : ChangeCategory.values()) {
      Set<String> aspects = new HashSet<>();
      switch (elementName) {
        case OWNERSHIP:
          {
            aspects.add(OWNERSHIP_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDataProduct,
                elementName,
                OWNERSHIP_ASPECT_NAME,
                new OwnershipChangeEventGenerator());
          }
          break;
        case DOCUMENTATION:
          {
            aspects.add(DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDataProduct,
                elementName,
                DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
                new DataProductPropertiesChangeEventGenerator());
          }
          break;
        case TAG:
          {
            aspects.add(GLOBAL_TAGS_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDataProduct,
                elementName,
                GLOBAL_TAGS_ASPECT_NAME,
                new GlobalTagsChangeEventGenerator());
          }
          break;
        case GLOSSARY_TERM:
          {
            aspects.add(GLOSSARY_TERMS_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDataProduct,
                elementName,
                GLOSSARY_TERMS_ASPECT_NAME,
                new GlossaryTermsChangeEventGenerator());
          }
          break;
        case DOMAIN:
          {
            aspects.add(DOMAINS_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDataProduct,
                elementName,
                DOMAINS_ASPECT_NAME,
                new SingleDomainChangeEventGenerator());
          }
          break;
        case STRUCTURED_PROPERTY:
          {
            aspects.add(STRUCTURED_PROPERTIES_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDataProduct,
                elementName,
                STRUCTURED_PROPERTIES_ASPECT_NAME,
                new StructuredPropertyChangeEventGenerator());
          }
          break;
        case APPLICATION:
          {
            aspects.add(APPLICATION_MEMBERSHIP_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDataProduct,
                elementName,
                APPLICATION_MEMBERSHIP_ASPECT_NAME,
                new ApplicationsChangeEventGenerator());
          }
          break;
        case ASSET_MEMBERSHIP:
          {
            aspects.add(DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDataProduct,
                elementName,
                DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
                new DataProductPropertiesChangeEventGenerator());
          }
          break;
        default:
          break;
      }
      dataProductElementAspectRegistry.put(elementName, aspects);
    }

    // Document registry
    HashMap<ChangeCategory, Set<String>> documentElementAspectRegistry = new HashMap<>();
    String entityTypeDocument = DOCUMENT_ENTITY_NAME;
    for (ChangeCategory elementName : ChangeCategory.values()) {
      Set<String> aspects = new HashSet<>();
      switch (elementName) {
        case LIFECYCLE:
        case DOCUMENTATION:
        case PARENT:
        case RELATED_ENTITIES:
          {
            // DocumentInfo handles all these categories
            aspects.add(DOCUMENT_INFO_ASPECT_NAME);
            _entityChangeEventGeneratorFactory.addGenerator(
                entityTypeDocument,
                elementName,
                DOCUMENT_INFO_ASPECT_NAME,
                new DocumentInfoChangeEventGenerator());
          }
          break;
        default:
          break;
      }
      documentElementAspectRegistry.put(elementName, aspects);
    }

    entityTypeElementAspectRegistry.put(DATASET_ENTITY_NAME, datasetElementAspectRegistry);
    entityTypeElementAspectRegistry.put(
        GLOSSARY_TERM_ENTITY_NAME, glossaryTermElementAspectRegistry);
    entityTypeElementAspectRegistry.put(DOMAIN_ENTITY_NAME, domainElementAspectRegistry);
    entityTypeElementAspectRegistry.put(DATA_PRODUCT_ENTITY_NAME, dataProductElementAspectRegistry);
    entityTypeElementAspectRegistry.put(DOCUMENT_ENTITY_NAME, documentElementAspectRegistry);
  }

  Set<String> getAspectsFromElements(String entityType, Set<ChangeCategory> elementNames) {
    if (this.entityTypeElementAspectRegistry.containsKey(entityType)) {
      return elementNames.stream()
          .map(x -> entityTypeElementAspectRegistry.get(entityType).get(x))
          .flatMap(Collection::stream)
          .collect(Collectors.toSet());
    } else {
      throw new UnsupportedOperationException("Entity Type " + entityType + " not supported");
    }
  }

  @Nonnull
  @Override
  public List<ChangeTransaction> getTimeline(
      @Nonnull final Urn urn,
      @Nonnull final Set<ChangeCategory> elementNames,
      long startTimeMillis,
      long endTimeMillis,
      String startVersionStamp,
      String endVersionStamp,
      boolean rawDiffRequested) {

    Set<String> aspectNames = getAspectsFromElements(urn.getEntityType(), elementNames);

    // TODO: Add more logic for defaults
    if (startVersionStamp != null && startTimeMillis != 0) {
      throw new IllegalArgumentException(
          "Cannot specify both VersionStamp start and timestamp start");
    }

    if (endTimeMillis == 0) {
      endTimeMillis = System.currentTimeMillis();
    }
    if (startTimeMillis == -1) {
      startTimeMillis = endTimeMillis - DEFAULT_LOOKBACK_TIME_WINDOW_MILLIS;
    }

    // Pull full list of aspects for entity and filter timeseries aspects for range
    // query
    EntitySpec entitySpec = _entityRegistry.getEntitySpec(urn.getEntityType());
    List<AspectSpec> aspectSpecs = entitySpec.getAspectSpecs();
    Set<String> fullAspectNames =
        aspectSpecs.stream()
            .filter(aspectSpec -> !aspectSpec.isTimeseries())
            .map(AspectSpec::getName)
            .collect(Collectors.toSet());
    List<EntityAspect> aspectsInRange =
        this._aspectDao.getAspectsInRange(urn, fullAspectNames, startTimeMillis, endTimeMillis);

    return processAspectTimeline(
        urn, elementNames, aspectNames, fullAspectNames, aspectsInRange, rawDiffRequested, -1);
  }

  @Nonnull
  @Override
  public List<ChangeTransaction> getTimeline(
      @Nonnull final Urn urn,
      @Nonnull final Set<ChangeCategory> elementNames,
      int maxChangeTransactions,
      boolean rawDiffRequested) {

    Set<String> aspectNames = getAspectsFromElements(urn.getEntityType(), elementNames);

    EntitySpec entitySpec = _entityRegistry.getEntitySpec(urn.getEntityType());
    List<AspectSpec> aspectSpecs = entitySpec.getAspectSpecs();
    Set<String> fullAspectNames =
        aspectSpecs.stream()
            .filter(aspectSpec -> !aspectSpec.isTimeseries())
            .map(AspectSpec::getName)
            .collect(Collectors.toSet());

    int maxAspectRows = maxChangeTransactions * ASPECT_ROW_MULTIPLIER;
    List<EntityAspect> latestAspects =
        this._aspectDao.getLatestAspects(urn, fullAspectNames, maxAspectRows);

    return processAspectTimeline(
        urn,
        elementNames,
        aspectNames,
        fullAspectNames,
        latestAspects,
        rawDiffRequested,
        maxChangeTransactions);
  }

  private List<ChangeTransaction> processAspectTimeline(
      @Nonnull final Urn urn,
      @Nonnull final Set<ChangeCategory> elementNames,
      @Nonnull final Set<String> aspectNames,
      @Nonnull final Set<String> fullAspectNames,
      @Nonnull final List<EntityAspect> fetchedAspects,
      boolean rawDiffRequested,
      int maxChangeTransactions) {

    Map<String, TreeSet<EntityAspect>> aspectRowSetMap =
        constructAspectRowSetMap(urn, fullAspectNames, fetchedAspects);

    Map<Long, SortedMap<String, Long>> timestampVersionCache =
        constructTimestampVersionCache(aspectRowSetMap);

    SortedMap<Long, List<ChangeTransaction>> semanticDiffs =
        aspectRowSetMap.entrySet().stream()
            .filter(entry -> aspectNames.contains(entry.getKey()))
            .map(Map.Entry::getValue)
            .map(value -> computeDiffs(value, urn.getEntityType(), elementNames, rawDiffRequested))
            .collect(
                TreeMap::new,
                this::combineComputedDiffsPerTransactionId,
                this::combineComputedDiffsPerTransactionId);

    assignSemanticVersions(semanticDiffs);
    List<ChangeTransaction> changeTransactions =
        semanticDiffs.values().stream()
            .collect(ArrayList::new, ArrayList::addAll, ArrayList::addAll);
    List<ChangeTransaction> combinedChangeTransactions =
        combineTransactionsByTimestamp(changeTransactions, timestampVersionCache);
    combinedChangeTransactions.sort(Comparator.comparing(ChangeTransaction::getTimestamp));

    if (maxChangeTransactions > 0 && combinedChangeTransactions.size() > maxChangeTransactions) {
      combinedChangeTransactions =
          combinedChangeTransactions.subList(
              combinedChangeTransactions.size() - maxChangeTransactions,
              combinedChangeTransactions.size());
    }

    return combinedChangeTransactions;
  }

  /**
   * Constructs a map from aspect name to a sorted set of DB aspects by created timestamp. Set
   * includes all aspects relevant to an entity and does a lookback by 1 for all aspects, creating
   * sentinel values for when the oldest aspect possible has been retrieved or no value exists in
   * the DB for an aspect
   *
   * @param urn urn of the entity
   * @param fullAspectNames full list of aspects relevant to the entity
   * @param aspectsInRange aspects returned by the range query by timestampm
   * @return map constructed as described
   */
  private Map<String, TreeSet<EntityAspect>> constructAspectRowSetMap(
      Urn urn, Set<String> fullAspectNames, List<EntityAspect> aspectsInRange) {
    Map<String, TreeSet<EntityAspect>> aspectRowSetMap = new HashMap<>();
    fullAspectNames.forEach(
        aspectName ->
            aspectRowSetMap.put(
                aspectName, new TreeSet<>(Comparator.comparing(EntityAspect::getCreatedOn))));
    aspectsInRange.forEach(
        row -> {
          TreeSet<EntityAspect> rowList = aspectRowSetMap.get(row.getAspect());
          rowList.add(row);
        });

    // we need to pull previous versions of these aspects that are currently at a 0
    Map<String, Long> nextVersions = _aspectDao.getNextVersions(urn.toString(), fullAspectNames);

    for (Map.Entry<String, TreeSet<EntityAspect>> aspectMinVersion : aspectRowSetMap.entrySet()) {
      TreeSet<EntityAspect> aspectSet = aspectMinVersion.getValue();

      EntityAspect oldestAspect = null;
      if (!aspectSet.isEmpty()) {
        oldestAspect = aspectMinVersion.getValue().first();
      }
      Long nextVersion = nextVersions.get(aspectMinVersion.getKey());
      // Fill out sentinel value if the oldest value possible has been retrieved, else
      // get previous version prior to time range
      if (oldestAspect != null && isOldestPossible(oldestAspect, nextVersion)) {
        aspectMinVersion.getValue().add(createSentinel(aspectMinVersion.getKey()));
      } else {
        // get the next version
        long versionToGet = 0;
        if (oldestAspect != null) {
          versionToGet =
              (oldestAspect.getVersion() == 0L) ? nextVersion - 1 : oldestAspect.getVersion() - 1;
        }
        EntityAspect row =
            _aspectDao.getAspect(urn.toString(), aspectMinVersion.getKey(), versionToGet);
        if (row != null) {
          aspectRowSetMap.get(row.getAspect()).add(row);
        } else {
          aspectMinVersion.getValue().add(createSentinel(aspectMinVersion.getKey()));
        }
      }
    }
    return aspectRowSetMap;
  }

  private boolean isOldestPossible(EntityAspect oldestAspect, long nextVersion) {
    return (((oldestAspect.getVersion() == 0L) && (nextVersion == 1L))
        || (oldestAspect.getVersion() == 1L));
  }

  private MissingEntityAspect createSentinel(String aspectName) {
    MissingEntityAspect sentinel = new MissingEntityAspect();
    sentinel.setAspect(aspectName);
    sentinel.setCreatedOn(new Timestamp(0L));
    sentinel.setVersion(-1);
    return sentinel;
  }

  /**
   * Constructs a map from timestamp to a sorted map of aspect name -> version for use in
   * constructing the version stamp
   *
   * @param aspectRowSetMap map constructed as described in {@link
   *     TimelineServiceImpl#constructAspectRowSetMap}
   * @return map as described
   */
  private Map<Long, SortedMap<String, Long>> constructTimestampVersionCache(
      Map<String, TreeSet<EntityAspect>> aspectRowSetMap) {
    Set<EntityAspect> aspects =
        aspectRowSetMap.values().stream()
            .flatMap(TreeSet::stream)
            .filter(aspect -> aspect.getVersion() != -1L)
            .collect(Collectors.toSet());
    Map<Long, SortedMap<String, Long>> timestampVersionCache = new HashMap<>();
    for (EntityAspect aspect : aspects) {
      if (timestampVersionCache.containsKey(aspect.getCreatedOn().getTime())) {
        continue;
      }
      SortedMap<String, Long> versionStampMap = new TreeMap<>(Comparator.naturalOrder());
      for (TreeSet<EntityAspect> aspectSet : aspectRowSetMap.values()) {
        EntityAspect maybeMatch = null;
        for (EntityAspect aspect2 : aspectSet) {
          if (aspect2 instanceof MissingEntityAspect) {
            continue;
          }
          if (aspect.getCreatedOn().getTime() < aspect2.getCreatedOn().getTime()) {
            // Can't match a higher timestamp, fall back to maybeMatch or empty
            break;
          }
          if (aspect.getCreatedOn().getTime() == aspect2.getCreatedOn().getTime()) {
            versionStampMap.put(aspect2.getAspect(), aspect2.getVersion());
            maybeMatch = null;
            break;
          } else {
            maybeMatch = aspect2;
          }
        }
        if (maybeMatch != null) {
          versionStampMap.put(maybeMatch.getAspect(), maybeMatch.getVersion());
        }
      }
      timestampVersionCache.put(aspect.getCreatedOn().getTime(), versionStampMap);
    }
    return timestampVersionCache;
  }

  private SortedMap<Long, List<ChangeTransaction>> computeDiffs(
      TreeSet<EntityAspect> aspectTimeline,
      String entityType,
      Set<ChangeCategory> elementNames,
      boolean rawDiffsRequested) {
    EntityAspect previousValue = null;
    SortedMap<Long, List<ChangeTransaction>> changeTransactionsMap = new TreeMap<>();
    long transactionId;
    for (EntityAspect currentValue : aspectTimeline) {
      transactionId = currentValue.getCreatedOn().getTime();
      if (previousValue != null) {
        // we skip the first element and only compare once we have two in hand
        changeTransactionsMap.put(
            transactionId,
            computeDiff(previousValue, currentValue, entityType, elementNames, rawDiffsRequested));
      }
      previousValue = currentValue;
    }
    return changeTransactionsMap;
  }

  private List<ChangeTransaction> computeDiff(
      @Nonnull EntityAspect previousValue,
      @Nonnull EntityAspect currentValue,
      String entityType,
      Set<ChangeCategory> elementNames,
      boolean rawDiffsRequested) {
    String aspectName = currentValue.getAspect();

    List<ChangeTransaction> semanticChangeTransactions = new ArrayList<>();
    JsonPatch rawDiff = getRawDiff(previousValue, currentValue);
    for (ChangeCategory element : elementNames) {
      EntityChangeEventGenerator entityChangeEventGenerator;
      entityChangeEventGenerator =
          _entityChangeEventGeneratorFactory.getGenerator(entityType, element, aspectName);
      if (entityChangeEventGenerator != null) {
        try {
          ChangeTransaction changeTransaction =
              entityChangeEventGenerator.getSemanticDiff(
                  previousValue, currentValue, element, rawDiff, rawDiffsRequested);
          if (CollectionUtils.isNotEmpty(changeTransaction.getChangeEvents())) {
            semanticChangeTransactions.add(changeTransaction);
          }
        } catch (Exception e) {
          log.error(
              "Failed to compute semantic diff for entity={}, aspect={}, element={}",
              currentValue.getUrn(),
              aspectName,
              element,
              e);
          semanticChangeTransactions.add(
              ChangeTransaction.builder()
                  .semVerChange(SemanticChangeType.EXCEPTIONAL)
                  .changeEvents(
                      Collections.singletonList(
                          ChangeEvent.builder()
                              .description(
                                  String.format("%s:%s", e.getClass().getName(), e.getMessage()))
                              .build()))
                  .build());
        }
      }
    }
    return semanticChangeTransactions;
  }

  private JsonPatch getRawDiff(EntityAspect previousValue, EntityAspect currentValue) {
    JsonValue prevNode = Json.createReader(new StringReader("{}")).readValue();
    if (previousValue.getVersion() != -1) {
      prevNode = Json.createReader(new StringReader(previousValue.getMetadata())).readValue();
    }
    JsonValue currNode =
        Json.createReader(new StringReader(currentValue.getMetadata())).readValue();
    return Json.createDiff(prevNode.asJsonObject(), currNode.asJsonObject());
  }

  private void combineComputedDiffsPerTransactionId(
      @Nonnull SortedMap<Long, List<ChangeTransaction>> semanticDiffs,
      @Nonnull SortedMap<Long, List<ChangeTransaction>> computedDiffs) {
    for (Map.Entry<Long, List<ChangeTransaction>> entry : computedDiffs.entrySet()) {
      if (!semanticDiffs.containsKey(entry.getKey())) {
        semanticDiffs.put(entry.getKey(), entry.getValue());
      } else {
        List<ChangeTransaction> transactions = semanticDiffs.get(entry.getKey());
        transactions.addAll(entry.getValue());
        semanticDiffs.put(entry.getKey(), transactions);
      }
    }
  }

  private void assignSemanticVersions(
      SortedMap<Long, List<ChangeTransaction>> changeTransactionsMap) {
    SemanticVersion curGroupVersion = null;
    long transactionId = FIRST_TRANSACTION_ID - 1;
    for (Map.Entry<Long, List<ChangeTransaction>> entry : changeTransactionsMap.entrySet()) {
      if (transactionId >= entry.getKey()) {
        throw new IllegalArgumentException(
            String.format(
                "transactionId should be < previous. %s >= %s", transactionId, entry.getKey()));
      }
      transactionId = entry.getKey();
      SemanticChangeType highestChangeInGroup = SemanticChangeType.NONE;
      ChangeTransaction highestChangeTransaction =
          entry.getValue().stream()
              .max(Comparator.comparing(ChangeTransaction::getSemVerChange))
              .orElse(null);
      if (highestChangeTransaction != null) {
        highestChangeInGroup = highestChangeTransaction.getSemVerChange();
      }
      curGroupVersion = getGroupSemanticVersion(highestChangeInGroup, curGroupVersion);
      for (ChangeTransaction t : entry.getValue()) {
        t.setSemanticVersion(curGroupVersion.toString());
      }
    }
  }

  private SemanticVersion getGroupSemanticVersion(
      SemanticChangeType highestChangeInGroup, SemanticVersion previousVersion) {
    if (previousVersion == null) {
      // Start with all 0s if there is no previous version.
      return SemanticVersion.builder()
          .majorVersion(0)
          .minorVersion(0)
          .patchVersion(0)
          .qualifier(BUILD_VALUE_COMPUTED)
          .build();
    }
    // Evaluate the version for this group based on previous version and the
    // hightest semantic change type in the group.
    if (highestChangeInGroup == SemanticChangeType.MAJOR) {
      // Bump up major, reset all lower to 0s.
      return SemanticVersion.builder()
          .majorVersion(previousVersion.getMajorVersion() + 1)
          .minorVersion(0)
          .patchVersion(0)
          .qualifier(BUILD_VALUE_COMPUTED)
          .build();
    } else if (highestChangeInGroup == SemanticChangeType.MINOR) {
      // Bump up minor, reset all lower to 0s.
      return SemanticVersion.builder()
          .majorVersion(previousVersion.getMajorVersion())
          .minorVersion(previousVersion.getMinorVersion() + 1)
          .patchVersion(0)
          .qualifier(BUILD_VALUE_COMPUTED)
          .build();
    } else if (highestChangeInGroup == SemanticChangeType.PATCH) {
      // Bump up patch.
      return SemanticVersion.builder()
          .majorVersion(previousVersion.getMajorVersion())
          .minorVersion(previousVersion.getMinorVersion())
          .patchVersion(previousVersion.getPatchVersion() + 1)
          .qualifier(BUILD_VALUE_COMPUTED)
          .build();
    }
    return previousVersion;
  }

  private List<ChangeTransaction> combineTransactionsByTimestamp(
      List<ChangeTransaction> changeTransactions,
      Map<Long, SortedMap<String, Long>> timestampVersionCache) {
    Map<Long, List<ChangeTransaction>> transactionsByTimestamp =
        changeTransactions.stream().collect(Collectors.groupingBy(ChangeTransaction::getTimestamp));
    List<ChangeTransaction> combinedChangeTransactions = new ArrayList<>();
    for (List<ChangeTransaction> transactionList : transactionsByTimestamp.values()) {
      if (!transactionList.isEmpty()) {
        ChangeTransaction result = transactionList.get(0);
        SemanticChangeType maxSemanticChangeType = result.getSemVerChange();
        String maxSemVer = result.getSemVer();
        // Copy into a mutable list; some generators return unmodifiable lists
        List<ChangeEvent> mergedEvents = new ArrayList<>(result.getChangeEvents());
        for (int i = 1; i < transactionList.size(); i++) {
          ChangeTransaction element = transactionList.get(i);
          mergedEvents.addAll(element.getChangeEvents());
          maxSemanticChangeType =
              maxSemanticChangeType.compareTo(element.getSemVerChange()) >= 0
                  ? maxSemanticChangeType
                  : element.getSemVerChange();
          maxSemVer =
              maxSemVer.compareTo(element.getSemVer()) >= 0 ? maxSemVer : element.getSemVer();
        }
        result.setChangeEvents(mergedEvents);
        result.setSemVerChange(maxSemanticChangeType);
        result.setSemanticVersion(maxSemVer);
        SortedMap<String, Long> versionStampMap = timestampVersionCache.get(result.getTimestamp());
        result.setVersionStamp(
            versionStampMap != null ? constructVersionStamp(versionStampMap) : "");
        combinedChangeTransactions.add(result);
      }
    }
    return combinedChangeTransactions;
  }
}
