package io.datahubproject.openlineage.converter;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Operation;
import com.linkedin.common.OperationType;
import com.linkedin.common.Ownership;
import com.linkedin.common.Siblings;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetFieldProfile;
import com.linkedin.dataset.DatasetFieldProfileArray;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.FineGrainedLineageDownstreamType;
import com.linkedin.dataset.FineGrainedLineageUpstreamType;
import com.linkedin.dataset.Quantile;
import com.linkedin.dataset.QuantileArray;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.key.ContainerKey;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.EnumType;
import com.linkedin.schema.MapType;
import com.linkedin.schema.MySqlDDL;
import com.linkedin.schema.NullType;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.OtherSchema;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.schema.TimeType;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.DatahubDataset;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.datahubproject.openlineage.utils.DatahubUtils;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class OpenLineageDatasetMapper {

  static final String TABLE_PREFIX = "table/";

  private interface StringValueSetter {
    void set(String value);
  }

  private OpenLineageDatasetMapper() {}

  static List<MetadataChangeProposal> convertDatasetEventToMcps(
      OpenLineage.DatasetEvent event, DatahubOpenlineageConfig datahubConf) throws IOException {
    if (event.getDataset().getFacets() != null) {
      OpenLineageMappingUtils.logFacetNames(
          "DatasetEvent",
          "dataset",
          event.getDataset().getFacets().getAdditionalProperties().keySet());
    }
    return convertDatasetToMcps(event.getDataset(), datahubConf, true);
  }

  static List<MetadataChangeProposal> convertDatasetToMcps(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig datahubConf) throws IOException {
    return convertDatasetToMcps(dataset, datahubConf, false);
  }

  private static List<MetadataChangeProposal> convertDatasetToMcps(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig datahubConf, boolean datasetEvent)
      throws IOException {
    List<MetadataChangeProposal> mcps = new LinkedList<>();
    Optional<DatasetUrn> datasetUrn =
        OpenLineagePlatformResolver.convertOpenlineageDatasetToDatasetUrn(dataset, datahubConf);
    if (datasetUrn.isEmpty()) {
      datasetUrn =
          Optional.of(
              DatahubUtils.createDatasetUrn(
                  "unknown",
                  null,
                  dataset.getNamespace() + "/" + dataset.getName(),
                  datahubConf.getFabricType()));
    }
    DatahubDataset datahubDataset =
        mapDataset(
            dataset, datasetUrn.get(), datahubConf, datahubConf.isIncludeSchemaMetadata(), null);
    Status lifecycleStatus = datahubDataset.getStatus();
    if (datahubConf.isMaterializeDataset() || datasetEvent) {
      mcps.add(OpenLineageMcpFactory.convert(DatahubJob.materializeDataset(datasetUrn.get())));
      OpenLineageMappingUtils.addAspectToMcps(
          datasetUrn.get(),
          DatahubJob.DATASET_ENTITY_TYPE,
          lifecycleStatus != null ? lifecycleStatus : new Status().setRemoved(false),
          mcps);
    } else if (lifecycleStatus != null) {
      OpenLineageMappingUtils.addAspectToMcps(
          datasetUrn.get(), DatahubJob.DATASET_ENTITY_TYPE, lifecycleStatus, mcps);
    }

    if (datahubDataset.getSchemaMetadata() != null && datahubConf.isIncludeSchemaMetadata()) {
      OpenLineageMappingUtils.addAspectToMcps(
          datasetUrn.get(),
          DatahubJob.DATASET_ENTITY_TYPE,
          datahubDataset.getSchemaMetadata(),
          mcps);
    }

    if (datahubDataset.getDatasetProperties() != null) {
      OpenLineageMappingUtils.addAspectToMcps(
          datasetUrn.get(),
          DatahubJob.DATASET_ENTITY_TYPE,
          datahubDataset.getDatasetProperties(),
          mcps);
    }

    if (datahubDataset.getDataPlatformInstance() != null) {
      OpenLineageMappingUtils.addAspectToMcps(
          datasetUrn.get(),
          DatahubJob.DATASET_ENTITY_TYPE,
          datahubDataset.getDataPlatformInstance(),
          mcps);
    }

    if (datahubDataset.getSiblings() != null) {
      OpenLineageMappingUtils.addAspectToMcps(
          datasetUrn.get(), DatahubJob.DATASET_ENTITY_TYPE, datahubDataset.getSiblings(), mcps);
    }

    if (datahubDataset.getOwnership() != null) {
      OpenLineageMappingUtils.addAspectToMcps(
          datasetUrn.get(), DatahubJob.DATASET_ENTITY_TYPE, datahubDataset.getOwnership(), mcps);
    }

    if (datahubDataset.getGlobalTags() != null) {
      OpenLineageMappingUtils.addAspectToMcps(
          datasetUrn.get(), DatahubJob.DATASET_ENTITY_TYPE, datahubDataset.getGlobalTags(), mcps);
    }

    if (datahubDataset.getSubTypes() != null) {
      OpenLineageMappingUtils.addAspectToMcps(
          datasetUrn.get(), DatahubJob.DATASET_ENTITY_TYPE, datahubDataset.getSubTypes(), mcps);
    }
    addHierarchyMcps(mcps, dataset, datasetUrn.get());
    return mcps;
  }

  private static DatahubDataset mapDataset(
      OpenLineage.Dataset dataset,
      DatasetUrn datasetUrn,
      DatahubOpenlineageConfig datahubConf,
      boolean includeSchemaMetadata,
      OpenLineage.Job job) {
    DatahubDataset.DatahubDatasetBuilder builder = DatahubDataset.builder().urn(datasetUrn);
    builder.datasetProperties(getDatasetProperties(dataset));
    builder.dataPlatformInstance(getDatasetDataPlatformInstance(dataset, datasetUrn, datahubConf));
    builder.ownership(generateDatasetOwnership(dataset.getFacets()));
    builder.globalTags(generateDatasetTags(dataset.getFacets()));
    builder.subTypes(generateDatasetSubTypes(dataset.getFacets()));
    builder.siblings(getDatasetSiblings(dataset, datasetUrn, datahubConf));
    builder.status(getDatasetStatus(dataset));
    if (includeSchemaMetadata) {
      builder.schemaMetadata(getSchemaMetadata(dataset, datahubConf));
    }
    if (datahubConf.isCaptureColumnLevelLineage() && job != null) {
      builder.lineage(getFineGrainedLineage(dataset, datahubConf, job));
    }
    return builder.build();
  }

  static void addHierarchyMcps(
      List<MetadataChangeProposal> mcps, OpenLineage.Dataset dataset, DatasetUrn datasetUrn) {
    if (dataset.getFacets() == null || dataset.getFacets().getHierarchy() == null) {
      return;
    }
    List<OpenLineage.HierarchyDatasetFacetLevel> hierarchy =
        dataset.getFacets().getHierarchy().getHierarchy();
    if (hierarchy == null || hierarchy.size() < 2) {
      return;
    }
    if (hierarchy.stream()
        .anyMatch(
            level ->
                level == null
                    || level.getType() == null
                    || level.getType().isBlank()
                    || level.getName() == null
                    || level.getName().isBlank())) {
      log.debug("Skipping invalid OpenLineage hierarchy facet for dataset {}", datasetUrn);
      return;
    }

    Urn parentContainerUrn = null;
    List<String> qualifiedPath = new LinkedList<>();
    StringBuilder identityPath = new StringBuilder();
    for (int index = 0; index < hierarchy.size() - 1; index++) {
      OpenLineage.HierarchyDatasetFacetLevel level = hierarchy.get(index);
      qualifiedPath.add(level.getName());
      identityPath
          .append('/')
          .append(level.getType().toUpperCase(Locale.ROOT))
          .append('=')
          .append(level.getName());
      String identity =
          datasetUrn.getPlatformEntity().getPlatformNameEntity()
              + '|'
              + datasetUrn.getOriginEntity()
              + '|'
              + dataset.getNamespace()
              + identityPath;
      String guid =
          UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8))
              .toString()
              .replace("-", "");
      Urn containerUrn = UrnUtils.getUrn("urn:li:container:" + guid);

      addMcpIfAbsent(mcps, containerUrn, "container", new ContainerKey().setGuid(guid));
      StringMap customProperties =
          new StringMap(
              Map.of(
                  "openlineage.hierarchy.type",
                  level.getType(),
                  "openlineage.hierarchy.namespace",
                  dataset.getNamespace()));
      ContainerProperties properties =
          new ContainerProperties()
              .setName(level.getName())
              .setQualifiedName(String.join(".", qualifiedPath))
              .setEnv(datasetUrn.getOriginEntity())
              .setCustomProperties(customProperties);
      addMcpIfAbsent(mcps, containerUrn, "container", properties);
      addMcpIfAbsent(mcps, containerUrn, "container", new Status().setRemoved(Boolean.FALSE));
      if (parentContainerUrn != null) {
        addMcpIfAbsent(
            mcps, containerUrn, "container", new Container().setContainer(parentContainerUrn));
      }
      parentContainerUrn = containerUrn;
    }

    if (parentContainerUrn != null) {
      addMcpIfAbsent(
          mcps,
          datasetUrn,
          DatahubJob.DATASET_ENTITY_TYPE,
          new Container().setContainer(parentContainerUrn));
    }
  }

  static void addMcpIfAbsent(
      List<MetadataChangeProposal> mcps,
      Urn entityUrn,
      String entityType,
      com.linkedin.data.template.DataTemplate aspect) {
    MetadataChangeProposal mcp = OpenLineageMcpFactory.upsert(entityUrn, entityType, aspect);
    boolean exists = mcps.stream().anyMatch(existing -> existing.equals(mcp));
    if (!exists) {
      mcps.add(mcp);
    }
  }

  static Status getDatasetStatus(OpenLineage.Dataset dataset) {
    if (dataset.getFacets() == null || dataset.getFacets().getLifecycleStateChange() == null) {
      return null;
    }
    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange =
        dataset.getFacets().getLifecycleStateChange().getLifecycleStateChange();
    if (lifecycleStateChange
        == OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP) {
      return new Status().setRemoved(true);
    }
    return null;
  }

  static Ownership generateDatasetOwnership(OpenLineage.DatasetFacets datasetFacets) {
    if (datasetFacets == null || datasetFacets.getOwnership() == null) {
      return null;
    }
    return OpenLineageMappingUtils.generateOwnership(
        datasetFacets.getOwnership().getOwners(),
        OpenLineage.OwnershipDatasetFacetOwners::getName,
        OpenLineage.OwnershipDatasetFacetOwners::getType);
  }

  static GlobalTags generateDatasetTags(OpenLineage.DatasetFacets datasetFacets) {
    if (datasetFacets == null || datasetFacets.getTags() == null) {
      return null;
    }
    return OpenLineageMappingUtils.generateFacetTags(
        datasetFacets.getTags().getTags(),
        OpenLineage.TagsDatasetFacetFields::getKey,
        OpenLineage.TagsDatasetFacetFields::getValue);
  }

  static SubTypes generateDatasetSubTypes(OpenLineage.DatasetFacets datasetFacets) {
    if (datasetFacets == null || datasetFacets.getDatasetType() == null) {
      return null;
    }
    LinkedHashSet<String> typeNames = new LinkedHashSet<>();
    if (datasetFacets.getDatasetType().getDatasetType() != null) {
      typeNames.add(datasetFacets.getDatasetType().getDatasetType());
    }
    if (datasetFacets.getDatasetType().getSubType() != null) {
      typeNames.add(datasetFacets.getDatasetType().getSubType());
    }
    if (typeNames.isEmpty()) {
      return null;
    }
    return new SubTypes().setTypeNames(new StringArray(new LinkedList<>(typeNames)));
  }

  static DatasetProperties getDatasetProperties(OpenLineage.Dataset dataset) {
    if (dataset.getFacets() == null) {
      return null;
    }

    DatasetProperties properties = new DatasetProperties();
    boolean hasProperties = false;
    if (dataset.getFacets().getDocumentation() != null
        && dataset.getFacets().getDocumentation().getDescription() != null) {
      properties.setDescription(dataset.getFacets().getDocumentation().getDescription());
      hasProperties = true;
    }
    if (dataset.getFacets().getDataSource() != null
        && dataset.getFacets().getDataSource().getUri() != null) {
      properties.setExternalUrl(new Url(dataset.getFacets().getDataSource().getUri().toString()));
      hasProperties = true;
    }

    StringMap customProperties = new StringMap();
    properties.setCustomProperties(customProperties);
    OpenLineage.StorageDatasetFacet storage = dataset.getFacets().getStorage();
    if (storage != null) {
      hasProperties |=
          OpenLineageMappingUtils.putIfPresent(
              customProperties, "storageLayer", storage.getStorageLayer());
      hasProperties |=
          OpenLineageMappingUtils.putIfPresent(
              customProperties, "fileFormat", storage.getFileFormat());
    }

    OpenLineage.CatalogDatasetFacet catalog = dataset.getFacets().getCatalog();
    if (catalog != null) {
      hasProperties |=
          OpenLineageMappingUtils.putIfPresent(
              customProperties, "catalogFramework", catalog.getFramework());
      hasProperties |=
          OpenLineageMappingUtils.putIfPresent(customProperties, "catalogType", catalog.getType());
      hasProperties |=
          OpenLineageMappingUtils.putIfPresent(customProperties, "catalogName", catalog.getName());
      hasProperties |=
          OpenLineageMappingUtils.putIfPresent(
              customProperties, "catalogMetadataUri", catalog.getMetadataUri());
      hasProperties |=
          OpenLineageMappingUtils.putIfPresent(
              customProperties, "catalogWarehouseUri", catalog.getWarehouseUri());
      hasProperties |=
          OpenLineageMappingUtils.putIfPresent(
              customProperties, "catalogSource", catalog.getSource());
    }

    OpenLineage.DatasetVersionDatasetFacet version = dataset.getFacets().getVersion();
    if (version != null) {
      hasProperties |=
          OpenLineageMappingUtils.putIfPresent(
              customProperties, "openlineage.datasetVersion", version.getDatasetVersion());
    }

    return hasProperties ? properties : null;
  }

  static DataPlatformInstance getDatasetDataPlatformInstance(
      OpenLineage.Dataset dataset, DatasetUrn datasetUrn, DatahubOpenlineageConfig datahubConf) {
    if (dataset.getFacets() == null) {
      return null;
    }

    String instance = datahubConf.getCommonDatasetPlatformInstance();
    OpenLineage.CatalogDatasetFacet catalog = dataset.getFacets().getCatalog();
    if ((instance == null || instance.isBlank()) && catalog != null) {
      instance = catalog.getName();
    } else if (instance != null
        && catalog != null
        && catalog.getName() != null
        && !catalog.getName().equals(instance)) {
      log.debug(
          "Skipping OpenLineage catalog instance '{}' for dataset '{}' because common dataset platform instance is configured",
          catalog.getName(),
          datasetUrn);
    }

    OpenLineage.DatasourceDatasetFacet dataSource = dataset.getFacets().getDataSource();
    if ((instance == null || instance.isBlank()) && dataSource != null) {
      instance = dataSource.getName();
    }

    if (instance == null || instance.isBlank()) {
      return null;
    }

    String platformName = datasetUrn.getPlatformEntity().getPlatformNameEntity();
    return new DataPlatformInstance()
        .setPlatform(datasetUrn.getPlatformEntity())
        .setInstance(OpenLineageMappingUtils.dataPlatformInstanceUrn(platformName, instance));
  }

  static Siblings getDatasetSiblings(
      OpenLineage.Dataset dataset, DatasetUrn resolvedUrn, DatahubOpenlineageConfig datahubConf) {
    if (dataset.getFacets() == null
        || dataset.getFacets().getSymlinks() == null
        || dataset.getFacets().getSymlinks().getIdentifiers() == null
        || dataset.getFacets().getSymlinks().getIdentifiers().isEmpty()) {
      return null;
    }

    UrnArray siblingUrns = new UrnArray();
    Optional<DatasetUrn> originalUrn =
        OpenLineagePlatformResolver.getDatasetUrnFromOlDataset(
            dataset.getNamespace(), dataset.getName(), null, datahubConf);
    originalUrn.ifPresent(urn -> addSiblingIfDifferent(siblingUrns, resolvedUrn, urn));

    for (OpenLineage.SymlinksDatasetFacetIdentifiers identifier :
        dataset.getFacets().getSymlinks().getIdentifiers()) {
      getDatasetUrnFromSymlinkIdentifier(identifier, datahubConf)
          .ifPresent(urn -> addSiblingIfDifferent(siblingUrns, resolvedUrn, urn));
    }

    if (siblingUrns.isEmpty()) {
      return null;
    }
    return new Siblings().setSiblings(siblingUrns).setPrimary(true);
  }

  static void addSiblingIfDifferent(
      UrnArray siblingUrns, DatasetUrn resolvedUrn, DatasetUrn siblingUrn) {
    if (resolvedUrn.toString().equals(siblingUrn.toString())) {
      return;
    }
    boolean alreadyPresent =
        siblingUrns.stream()
            .anyMatch(existing -> existing.toString().equals(siblingUrn.toString()));
    if (!alreadyPresent) {
      siblingUrns.add(siblingUrn);
    }
  }

  static Optional<DatasetUrn> getDatasetUrnFromSymlinkIdentifier(
      OpenLineage.SymlinksDatasetFacetIdentifiers identifier,
      DatahubOpenlineageConfig datahubConf) {
    if (!"TABLE".equals(identifier.getType())) {
      log.debug("Skipping unmapped OpenLineage symlink identifier type '{}'", identifier.getType());
      return Optional.empty();
    }
    if (identifier.getNamespace() == null || identifier.getName() == null) {
      return Optional.empty();
    }

    String namespace;
    if (identifier.getNamespace().startsWith("aws:glue:")
        || identifier.getNamespace().startsWith("arn:aws:glue:")) {
      namespace = "glue";
    } else {
      namespace = datahubConf.getHivePlatformAlias();
    }
    String name = identifier.getName();
    if (name != null && name.startsWith(TABLE_PREFIX)) {
      name = name.replaceFirst(TABLE_PREFIX, "").replace("/", ".");
    }
    return OpenLineagePlatformResolver.getDatasetUrnFromOlDataset(
        namespace,
        name,
        OpenLineagePlatformResolver.toConnectionKey(identifier.getNamespace()),
        datahubConf);
  }

  static Operation getInputOperation(OpenLineage.InputDataset input, ZonedDateTime eventTime)
      throws URISyntaxException {
    if (input.getInputFacets() == null || input.getInputFacets().getInputStatistics() == null) {
      return null;
    }

    OpenLineage.InputStatisticsInputDatasetFacet statistics =
        input.getInputFacets().getInputStatistics();
    Operation operation = createDatasetOperation(eventTime, "READ");
    if (statistics.getRowCount() != null) {
      operation.setNumAffectedRows(statistics.getRowCount());
    }
    StringMap customProperties = new StringMap();
    if (statistics.getSize() != null) {
      customProperties.put("numAffectedBytes", statistics.getSize().toString());
    }
    if (statistics.getFileCount() != null) {
      customProperties.put("fileCount", statistics.getFileCount().toString());
    }
    if (!customProperties.isEmpty()) {
      operation.setCustomProperties(customProperties);
    }
    return operation;
  }

  static DatasetProfile getDatasetProfile(OpenLineage.InputDataset input, ZonedDateTime eventTime) {
    if (input.getInputFacets() == null || input.getInputFacets().getDataQualityMetrics() == null) {
      return null;
    }

    OpenLineage.DataQualityMetricsInputDatasetFacet metrics =
        input.getInputFacets().getDataQualityMetrics();
    DatasetProfile datasetProfile = new DatasetProfile();
    datasetProfile.setTimestampMillis(
        eventTime != null ? eventTime.toInstant().toEpochMilli() : System.currentTimeMillis());
    boolean hasMetrics = false;
    if (metrics.getRowCount() != null) {
      datasetProfile.setRowCount(metrics.getRowCount());
      hasMetrics = true;
    }
    if (metrics.getBytes() != null) {
      datasetProfile.setSizeInBytes(metrics.getBytes());
      hasMetrics = true;
    }
    if (metrics.getColumnMetrics() != null
        && metrics.getColumnMetrics().getAdditionalProperties() != null) {
      DatasetFieldProfileArray fieldProfiles = new DatasetFieldProfileArray();
      metrics
          .getColumnMetrics()
          .getAdditionalProperties()
          .forEach(
              (fieldPath, columnMetrics) -> {
                DatasetFieldProfile fieldProfile = getDatasetFieldProfile(fieldPath, columnMetrics);
                if (fieldProfile != null) {
                  fieldProfiles.add(fieldProfile);
                }
              });
      datasetProfile.setColumnCount(
          (long) metrics.getColumnMetrics().getAdditionalProperties().size());
      if (!fieldProfiles.isEmpty()) {
        datasetProfile.setFieldProfiles(fieldProfiles);
        hasMetrics = true;
      }
    }
    return hasMetrics ? datasetProfile : null;
  }

  static DatasetFieldProfile getDatasetFieldProfile(
      String fieldPath,
      OpenLineage.DataQualityMetricsInputDatasetFacetColumnMetricsAdditional columnMetrics) {
    if (fieldPath == null || fieldPath.isBlank() || columnMetrics == null) {
      return null;
    }
    DatasetFieldProfile fieldProfile = new DatasetFieldProfile().setFieldPath(fieldPath);
    if (columnMetrics.getNullCount() != null) {
      fieldProfile.setNullCount(columnMetrics.getNullCount());
    }
    if (columnMetrics.getDistinctCount() != null) {
      fieldProfile.setUniqueCount(columnMetrics.getDistinctCount());
    }
    setFiniteString(columnMetrics.getMin(), fieldProfile::setMin);
    setFiniteString(columnMetrics.getMax(), fieldProfile::setMax);
    if (columnMetrics.getSum() != null
        && columnMetrics.getCount() != null
        && Double.isFinite(columnMetrics.getSum())
        && Double.isFinite(columnMetrics.getCount())
        && columnMetrics.getCount() != 0) {
      fieldProfile.setMean(Double.toString(columnMetrics.getSum() / columnMetrics.getCount()));
    }
    if (columnMetrics.getQuantiles() != null
        && columnMetrics.getQuantiles().getAdditionalProperties() != null) {
      QuantileArray quantiles = new QuantileArray();
      columnMetrics
          .getQuantiles()
          .getAdditionalProperties()
          .forEach(
              (quantile, value) -> {
                if (value != null && Double.isFinite(value)) {
                  quantiles.add(new Quantile().setQuantile(quantile).setValue(value.toString()));
                  if ("0.5".equals(quantile) || "0.50".equals(quantile)) {
                    fieldProfile.setMedian(value.toString());
                  }
                }
              });
      if (!quantiles.isEmpty()) {
        fieldProfile.setQuantiles(quantiles);
      }
    }
    return fieldProfile;
  }

  static void setFiniteString(Double value, StringValueSetter setter) {
    if (value != null && Double.isFinite(value)) {
      setter.set(value.toString());
    }
  }

  static Operation getOutputOperation(OpenLineage.OutputDataset output, OpenLineage.RunEvent event)
      throws URISyntaxException {
    OpenLineage.OutputStatisticsOutputDatasetFacet statistics =
        output.getOutputFacets() != null ? output.getOutputFacets().getOutputStatistics() : null;
    StringMap customProperties = new StringMap();
    boolean hasSqlProperties = addSqlOperationProperties(event, customProperties);
    boolean hasExternalQueryProperties =
        addExternalQueryOperationProperties(event, customProperties);
    if (statistics == null && !hasSqlProperties && !hasExternalQueryProperties) {
      return null;
    }

    Operation operation = createDatasetOperation(event.getEventTime(), "WRITE");
    if (statistics != null) {
      if (statistics.getRowCount() != null) {
        operation.setNumAffectedRows(statistics.getRowCount());
      }
      if (statistics.getSize() != null) {
        customProperties.put("numAffectedBytes", statistics.getSize().toString());
      }
      if (statistics.getFileCount() != null) {
        customProperties.put("fileCount", statistics.getFileCount().toString());
      }
    }
    if (!customProperties.isEmpty()) {
      operation.setCustomProperties(customProperties);
    }
    return operation;
  }

  static Operation createDatasetOperation(ZonedDateTime eventTime, String operationType)
      throws URISyntaxException {
    Operation operation = new Operation();
    operation.setOperationType(OperationType.CUSTOM);
    operation.setCustomOperationType(operationType);
    long timestampMillis =
        eventTime != null ? eventTime.toInstant().toEpochMilli() : System.currentTimeMillis();
    operation.setTimestampMillis(timestampMillis);
    operation.setLastUpdatedTimestamp(timestampMillis);
    operation.setActor(Urn.createFromString(OpenLineageMappingUtils.URN_LI_CORPUSER_DATAHUB));
    return operation;
  }

  static boolean addSqlOperationProperties(OpenLineage.RunEvent event, StringMap customProperties) {
    if (event.getJob().getFacets() == null
        || event.getJob().getFacets().getSql() == null
        || event.getJob().getFacets().getSql().getQuery() == null) {
      return false;
    }
    customProperties.put("queryStatement", event.getJob().getFacets().getSql().getQuery());
    return true;
  }

  static boolean addExternalQueryOperationProperties(
      OpenLineage.RunEvent event, StringMap customProperties) {
    if (event.getRun().getFacets() == null
        || event.getRun().getFacets().getExternalQuery() == null) {
      return false;
    }
    OpenLineage.ExternalQueryRunFacet externalQuery = event.getRun().getFacets().getExternalQuery();
    boolean hasProperties = false;
    if (externalQuery.getExternalQueryId() != null) {
      customProperties.put("externalQueryId", externalQuery.getExternalQueryId());
      hasProperties = true;
    }
    if (externalQuery.getSource() != null) {
      customProperties.put("externalQuerySource", externalQuery.getSource());
      hasProperties = true;
    }
    return hasProperties;
  }

  static UpstreamLineage getFineGrainedLineage(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig mappingConfig, OpenLineage.Job job) {
    FineGrainedLineageArray fgla = new FineGrainedLineageArray();
    UpstreamArray upstreams = new UpstreamArray();

    if ((dataset.getFacets() == null) || (dataset.getFacets().getColumnLineage() == null)) {
      return null;
    }

    OpenLineage.ColumnLineageDatasetFacet columnLineage = dataset.getFacets().getColumnLineage();
    // Per OpenLineage spec
    // (https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json),
    // "fields" is a required property. However, the OpenLineage Java client may return null
    // when the fields object is empty {} or when producers (like Trino) omit it.
    // We handle this gracefully to avoid NPE.
    if (columnLineage.getFields() == null) {
      log.warn(
          "ColumnLineageDatasetFacet has null fields for dataset '{}' - skipping fine-grained lineage extraction. "
              + "This may occur when the producer sends an empty fields object or omits it entirely.",
          dataset.getName());
      return null;
    }

    boolean includeIndirect = mappingConfig.isIncludeIndirectColumnLineage();

    Set<Map.Entry<String, OpenLineage.ColumnLineageDatasetFacetFieldsAdditional>> fields =
        columnLineage.getFields().getAdditionalProperties().entrySet();
    for (Map.Entry<String, OpenLineage.ColumnLineageDatasetFacetFieldsAdditional> field : fields) {
      FineGrainedLineage fgl = new FineGrainedLineage();

      UrnArray upstreamFields = new UrnArray();
      UrnArray downstreamsFields = new UrnArray();
      Optional<DatasetUrn> datasetUrn =
          OpenLineagePlatformResolver.convertOpenlineageDatasetToDatasetUrn(dataset, mappingConfig);
      datasetUrn.ifPresent(
          urn ->
              downstreamsFields.add(
                  UrnUtils.getUrn("urn:li:schemaField:" + "(" + urn + "," + field.getKey() + ")")));

      LinkedHashSet<String> transformationTexts = new LinkedHashSet<>();
      OpenLineage.StaticDatasetBuilder staticDatasetBuilder =
          new OpenLineage.StaticDatasetBuilder();
      field
          .getValue()
          .getInputFields()
          .forEach(
              inputField -> {
                // Capture transformation tags up front so the user can still see that the
                // SQL involves JOIN/FILTER/GROUP_BY operations even when we drop the URN
                // that contributed only an INDIRECT role.
                if (inputField.getTransformations() != null) {
                  for (OpenLineage.InputFieldTransformations transformation :
                      inputField.getTransformations()) {
                    transformationTexts.add(
                        String.format(
                            "%s:%s", transformation.getType(), transformation.getSubtype()));
                  }
                }

                // Drop input fields whose only role is INDIRECT (JOIN/FILTER/GROUP BY) when
                // the caller opts out. Mixed DIRECT+INDIRECT and DIRECT-only fields pass.
                if (!includeIndirect && isIndirectOnly(inputField)) {
                  return;
                }

                OpenLineage.Dataset staticDataset =
                    staticDatasetBuilder
                        .name(inputField.getName())
                        .namespace(inputField.getNamespace())
                        .build();
                Optional<DatasetUrn> urn =
                    OpenLineagePlatformResolver.convertOpenlineageDatasetToDatasetUrn(
                        staticDataset, mappingConfig);
                if (urn.isPresent()) {
                  Urn datasetFieldUrn =
                      UrnUtils.getUrn(
                          "urn:li:schemaField:"
                              + "("
                              + urn.get()
                              + ","
                              + inputField.getField()
                              + ")");
                  upstreamFields.add(datasetFieldUrn);
                  if (upstreams.stream()
                      .noneMatch(
                          upstream ->
                              upstream.getDataset().toString().equals(urn.get().toString()))) {
                    upstreams.add(
                        new Upstream()
                            .setDataset(urn.get())
                            .setType(DatasetLineageType.TRANSFORMED));
                  }
                }
              });

      if (upstreamFields.isEmpty()) {
        continue;
      }

      String combinedTransformations = "";

      // Capture transformation information from OpenLineage
      if (!transformationTexts.isEmpty()) {
        List<String> sortedList =
            transformationTexts.stream()
                .sorted(String.CASE_INSENSITIVE_ORDER)
                .collect(Collectors.toList());
        combinedTransformations = String.join(",", sortedList);
      }

      // Extract SQL query from SQLJobFacet if available
      if (job != null
          && job.getFacets() != null
          && job.getFacets().getSql() != null
          && job.getFacets().getSql().getQuery() != null) {
        String sqlQuery = job.getFacets().getSql().getQuery();
        if (!sqlQuery.trim().isEmpty()) {
          if (!combinedTransformations.isEmpty()) {
            combinedTransformations = "-- " + combinedTransformations + "\n" + sqlQuery;
          } else {
            combinedTransformations = sqlQuery;
          }
        }
      }

      upstreamFields.sort(Comparator.comparing(Urn::toString));
      fgl.setUpstreams(upstreamFields);
      fgl.setConfidenceScore(1.0f);
      fgl.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);

      downstreamsFields.sort(Comparator.comparing(Urn::toString));
      fgl.setDownstreams(downstreamsFields);
      fgl.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);
      fgl.setTransformOperation(combinedTransformations);
      fgla.add(fgl);
    }

    UpstreamLineage upstreamLineage = new UpstreamLineage();
    upstreamLineage.setFineGrainedLineages(fgla);
    upstreamLineage.setUpstreams(upstreams);
    return upstreamLineage;
  }

  static boolean isIndirectOnly(OpenLineage.InputField inputField) {
    List<OpenLineage.InputFieldTransformations> transformations = inputField.getTransformations();
    if (transformations == null || transformations.isEmpty()) {
      return false;
    }
    for (OpenLineage.InputFieldTransformations transformation : transformations) {
      if (!"INDIRECT".equalsIgnoreCase(transformation.getType())) {
        return false;
      }
    }
    return true;
  }

  static DatahubDataset mapInput(
      DatahubJob datahubJob,
      OpenLineage.RunEvent event,
      OpenLineage.InputDataset input,
      DatahubOpenlineageConfig datahubConf)
      throws URISyntaxException {
    Optional<DatasetUrn> datasetUrn =
        OpenLineagePlatformResolver.convertOpenlineageDatasetToDatasetUrn(input, datahubConf);
    if (datasetUrn.isEmpty()) {
      return null;
    }

    DatasetProfile datasetProfile = getDatasetProfile(input, event.getEventTime());
    if (datasetProfile != null) {
      datahubJob
          .getExtraMcps()
          .add(
              OpenLineageMcpFactory.upsert(
                  datasetUrn.get(), DatahubJob.DATASET_ENTITY_TYPE, datasetProfile));
    }
    Operation operation = getInputOperation(input, event.getEventTime());
    if (operation != null) {
      datahubJob
          .getExtraMcps()
          .add(
              OpenLineageMcpFactory.upsert(
                  datasetUrn.get(), DatahubJob.DATASET_ENTITY_TYPE, operation));
    }

    return mapDataset(
        input, datasetUrn.get(), datahubConf, datahubConf.isMaterializeDataset(), event.getJob());
  }

  static void applyInput(
      DatahubJob datahubJob, OpenLineage.InputDataset input, DatahubDataset datahubDataset) {
    addHierarchyMcps(datahubJob.getExtraMcps(), input, datahubDataset.getUrn());
    datahubJob.getInSet().add(datahubDataset);
  }

  static void processJobOutputs(
      DatahubJob datahubJob, OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf)
      throws URISyntaxException {

    if (event.getOutputs() == null) {
      return;
    }

    for (OpenLineage.OutputDataset output : event.getOutputs()) {
      Optional<DatasetUrn> datasetUrn =
          OpenLineagePlatformResolver.convertOpenlineageDatasetToDatasetUrn(output, datahubConf);
      if (datasetUrn.isPresent()) {
        Operation operation = getOutputOperation(output, event);
        if (operation != null) {
          datahubJob
              .getExtraMcps()
              .add(
                  OpenLineageMcpFactory.upsert(
                      datasetUrn.get(), DatahubJob.DATASET_ENTITY_TYPE, operation));
        }

        DatahubDataset datahubDataset =
            mapDataset(
                output,
                datasetUrn.get(),
                datahubConf,
                datahubConf.isMaterializeDataset(),
                event.getJob());
        addHierarchyMcps(datahubJob.getExtraMcps(), output, datasetUrn.get());
        datahubJob.getOutSet().add(datahubDataset);
      }
    }
  }

  static SchemaFieldDataType.Type convertOlFieldTypeToDHFieldType(String openLineageFieldType) {
    if (openLineageFieldType == null) {
      return SchemaFieldDataType.Type.create(new NullType());
    }
    switch (openLineageFieldType.toLowerCase(Locale.ROOT)) {
      case "string":
      case "varchar":
      case "char":
        return SchemaFieldDataType.Type.create(new StringType());
      case "boolean":
      case "bool":
        return SchemaFieldDataType.Type.create(new BooleanType());
      case "bytes":
      case "binary":
        return SchemaFieldDataType.Type.create(new BytesType());
      case "enum":
        return SchemaFieldDataType.Type.create(new EnumType());
      case "byte":
      case "short":
      case "long":
      case "int":
      case "integer":
      case "double":
      case "float":
      case "decimal":
      case "numeric":
        return SchemaFieldDataType.Type.create(new NumberType());
      case "timestamp":
      case "datetime":
      case "date":
      case "time":
        return SchemaFieldDataType.Type.create(new TimeType());
      case "struct":
      case "record":
      case "map":
      case "object":
        return SchemaFieldDataType.Type.create(new MapType());
      case "array":
      case "list":
        return SchemaFieldDataType.Type.create(new ArrayType());
      default:
        return SchemaFieldDataType.Type.create(new NullType());
    }
  }

  static SchemaMetadata getSchemaMetadata(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig mappingConfig) {
    SchemaFieldArray schemaFieldArray = new SchemaFieldArray();
    // Per OpenLineage spec (https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json),
    // "fields" is NOT a required property - it can be omitted entirely.
    // Producers like Trino may send SchemaDatasetFacet without fields.
    // We handle this gracefully to avoid NPE.
    if ((dataset.getFacets() == null)
        || (dataset.getFacets().getSchema() == null)
        || (dataset.getFacets().getSchema().getFields() == null)) {
      log.warn(
          "SchemaDatasetFacet has null or missing fields for dataset '{}' - skipping schema metadata extraction",
          dataset.getName());
      return null;
    }
    flattenSchemaFields(dataset.getFacets().getSchema().getFields(), null, schemaFieldArray);
    SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setPlatformSchema(new SchemaMetadata.PlatformSchema());
    schemaMetadata.setSchemaName("");
    schemaMetadata.setVersion(1L);
    schemaMetadata.setHash("");

    Optional<DatasetUrn> datasetUrn =
        OpenLineagePlatformResolver.getDatasetUrnFromOlDataset(
            dataset.getNamespace(), dataset.getName(), null, mappingConfig);

    if (!datasetUrn.isPresent()) {
      return null;
    }

    String rawSchema = OpenLineageClientUtils.toJson(dataset.getFacets().getSchema().getFields());
    SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    String platformName = datasetUrn.get().getPlatformEntity().getPlatformNameEntity();
    if ("mysql".equals(platformName) || "mariadb".equals(platformName)) {
      MySqlDDL ddl = new MySqlDDL();
      ddl.setTableSchema(rawSchema);
      platformSchema.setMySqlDDL(ddl);
    } else {
      platformSchema.setOtherSchema(new OtherSchema().setRawSchema(rawSchema));
    }

    schemaMetadata.setPlatformSchema(platformSchema);

    schemaMetadata.setPlatform(datasetUrn.get().getPlatformEntity());

    schemaMetadata.setFields(schemaFieldArray);
    return schemaMetadata;
  }

  static void flattenSchemaFields(
      List<OpenLineage.SchemaDatasetFacetFields> fields,
      String parentPath,
      SchemaFieldArray output) {
    if (fields == null) {
      return;
    }
    for (OpenLineage.SchemaDatasetFacetFields field : fields) {
      if (field.getName() == null || field.getName().isBlank()) {
        continue;
      }
      String fieldPath =
          parentPath == null || parentPath.isBlank()
              ? field.getName()
              : parentPath + "." + field.getName();
      SchemaField schemaField = new SchemaField();
      schemaField.setFieldPath(fieldPath);
      schemaField.setNativeDataType(field.getType() != null ? field.getType() : "unknown");
      schemaField.setType(
          new SchemaFieldDataType().setType(convertOlFieldTypeToDHFieldType(field.getType())));
      if (field.getDescription() != null) {
        schemaField.setDescription(field.getDescription());
      }
      output.add(schemaField);
      flattenSchemaFields(field.getFields(), fieldPath, output);
    }
  }
}
