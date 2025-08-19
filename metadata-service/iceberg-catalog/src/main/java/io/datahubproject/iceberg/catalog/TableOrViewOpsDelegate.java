package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;
import static io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse.*;
import static io.datahubproject.iceberg.catalog.Utils.*;
import static org.apache.commons.lang3.StringUtils.capitalize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.container.Container;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.IcebergCatalogInfo;
import com.linkedin.dataset.ViewProperties;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.patch.builder.DatasetPropertiesPatchBuilder;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.schematron.converters.avro.AvroSchemaConverter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.iceberg.view.ViewRepresentation;

@Slf4j
abstract class TableOrViewOpsDelegate<M> {

  private final DataHubIcebergWarehouse warehouse;
  private FileIO io;
  private final TableIdentifier tableIdentifier;
  private final EntityService entityService;
  private final OperationContext operationContext;
  private final FileIOFactory fileIOFactory;
  private volatile M currentMetadata = null;
  private volatile boolean shouldRefresh = true;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  TableOrViewOpsDelegate(
      DataHubIcebergWarehouse warehouse,
      TableIdentifier tableIdentifier,
      EntityService entityService,
      OperationContext operationContext,
      FileIOFactory fileIOFactory) {
    this.warehouse = warehouse;
    this.tableIdentifier = tableIdentifier;
    this.entityService = entityService;
    this.operationContext = operationContext;
    this.fileIOFactory = fileIOFactory;
  }

  public M refresh() {
    Optional<IcebergCatalogInfo> icebergMeta = warehouse.getIcebergMetadata(tableIdentifier);

    if (icebergMeta.isEmpty() || !isExpectedType(icebergMeta.get().isView())) {
      return null;
    }

    String location = icebergMeta.get().getMetadataPointer();
    if (io == null) {
      String locationDir = location.substring(0, location.lastIndexOf("/"));
      io =
          fileIOFactory.createIO(
              platformInstance(), PoliciesConfig.DATA_READ_ONLY_PRIVILEGE, Set.of(locationDir));
    }

    currentMetadata = readMetadata(io, location);
    shouldRefresh = false;
    return currentMetadata;
  }

  M current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  protected void doCommit(
      MetadataWrapper<M> base, MetadataWrapper<M> metadata, Supplier<String> metadataWriter) {

    Pair<EnvelopedAspect, DatasetUrn> existingDatasetAspect =
        warehouse.getIcebergMetadataEnveloped(tableIdentifier);

    boolean creation = base == null;

    if (existingDatasetAspect != null) {
      if (creation) {
        throw new AlreadyExistsException("%s already exists: %s", capitalize(type()), name());
      }
      IcebergCatalogInfo existingMetadata =
          new IcebergCatalogInfo(existingDatasetAspect.getFirst().getValue().data());
      if (!isExpectedType(existingMetadata.isView())) {
        throw noSuchEntityException();
      }
      if (!existingMetadata.getMetadataPointer().equals(base.metadataFileLocation())) {
        throw new CommitFailedException("Cannot commit to %s %s: stale metadata", type(), name());
      }
    }

    if (!creation && existingDatasetAspect == null) {
      throw new IllegalStateException(
          "Iceberg metadata aspect not found for "
              + tableIdentifier
              + " with base metadata-file "
              + base.location());
    }

    DatasetUrn datasetUrn;
    // attempt to commit
    io =
        fileIOFactory.createIO(
            platformInstance(),
            PoliciesConfig.DATA_READ_WRITE_PRIVILEGE,
            Set.of(metadata.location()));
    String newMetadataLocation = metadataWriter.get();

    IcebergBatch icebergBatch = newIcebergBatch(operationContext);

    if (creation) {
      try {
        datasetUrn = warehouse.createDataset(tableIdentifier, isView(), icebergBatch);
      } catch (ValidationException e) {
        throw new AlreadyExistsException("%s already exists: %s", capitalize(type()), name());
      }
    } else {
      datasetUrn = existingDatasetAspect.getSecond();
    }

    IcebergCatalogInfo icebergCatalogInfo =
        new IcebergCatalogInfo().setMetadataPointer(newMetadataLocation).setView(isView());
    IcebergBatch.EntityBatch datasetBatch;
    if (creation) {
      datasetBatch =
          icebergBatch.createEntity(
              datasetUrn,
              DATASET_ENTITY_NAME,
              DATASET_ICEBERG_METADATA_ASPECT_NAME,
              icebergCatalogInfo);
    } else {
      String existingVersion = existingDatasetAspect.getFirst().getSystemMetadata().getVersion();
      datasetBatch =
          icebergBatch.conditionalUpdateEntity(
              datasetUrn,
              DATASET_ENTITY_NAME,
              DATASET_ICEBERG_METADATA_ASPECT_NAME,
              icebergCatalogInfo,
              existingVersion);
    }

    if (base == null || (base.currentSchemaId() != metadata.currentSchemaId())) {
      // schema changed
      Schema avroSchema = AvroSchemaUtil.convert(metadata.schema(), name());
      AvroSchemaConverter converter = AvroSchemaConverter.builder().build();
      SchemaMetadata schemaMetadata =
          converter.toDataHubSchema(avroSchema, false, false, platformUrn(), null);
      // extend schema metadata with partition info
      if (metadata.tableMetadata != null) {
        PartitionSpec partitionSpec =
            metadata.tableMetadata.spec(metadata.tableMetadata.defaultSpecId());
        if (partitionSpec != null) {
          HashMap<String, SchemaField> fieldIdToSchemaFieldMap = new HashMap<>();
          for (SchemaField schemaField : schemaMetadata.getFields()) {
            if (schemaField.hasJsonProps()) {
              String jsonProps = schemaField.getJsonProps();
              Map<String, Object> jsonPropsMap = null;
              try {
                // jsonProps (and its source in avro) can have string key/values. Iceberg
                // spec for field-ids uses String key and integer values and these get
                // stored in jsonProps. There may be other custom properties we do not
                // process so using string key/value.
                jsonPropsMap =
                    OBJECT_MAPPER.readValue(jsonProps, new TypeReference<Map<String, Object>>() {});
                Object fieldId = jsonPropsMap.get("field-id");
                if (fieldId != null) {
                  fieldIdToSchemaFieldMap.put(fieldId.toString(), schemaField);
                }
              } catch (JsonProcessingException e) {
                // Lets not block table commit. This just means we wont have partition info
                // populated in our metadata. The compute engine can continue to use IRC
                // for this table.
                log.error("Failed to process partitioning information for table {}", name(), e);
              }
            }
          }
          for (PartitionField field : partitionSpec.fields()) {
            String fieldIdStr = String.valueOf(field.sourceId());
            if (fieldIdToSchemaFieldMap.containsKey(fieldIdStr)) {
              SchemaField datahubField = fieldIdToSchemaFieldMap.get(fieldIdStr);
              datahubField.setIsPartitioningKey(true);
            } else {
              // This shouldn't really happen per the spec, but we are referring to a
              // field id that was missing in the schema. Possible bug in schematron or
              // iceberg metadata is corrupt
              log.error(
                  "Internal error: Could not find field-id {} in schema for table {}",
                  fieldIdStr,
                  name());
            }
          }
        }
      }
      datasetBatch.aspect(SCHEMA_METADATA_ASPECT_NAME, schemaMetadata);
    }

    if (creation) {
      datasetBatch.platformInstance(platformInstance());

      DatasetProperties datasetProperties =
          new DatasetProperties().setName(tableIdentifier.name()).setQualifiedName(name());
      datasetBatch.aspect(DATASET_PROPERTIES_ASPECT_NAME, datasetProperties);

      Container container = new Container();
      container.setContainer(containerUrn(platformInstance(), tableIdentifier.namespace()));
      datasetBatch.aspect(CONTAINER_ASPECT_NAME, container);

      SubTypes subTypes = new SubTypes().setTypeNames(new StringArray(capitalize(type())));
      datasetBatch.aspect(SUB_TYPES_ASPECT_NAME, subTypes);

      BrowsePathsV2 browsePaths =
          Utils.browsePathsForContainer(platformInstance(), tableIdentifier.namespace().levels());
      datasetBatch.aspect(BROWSE_PATHS_V2_ASPECT_NAME, browsePaths);
    }

    additionalMcps(metadata.metadata(), datasetBatch);
    try {
      AspectsBatch aspectsBatch = icebergBatch.asAspectsBatch();
      entityService.ingestProposal(operationContext, aspectsBatch, false);
    } catch (ValidationException e) {
      if (creation) {
        // this is likely because table/view already exists i.e. created concurrently in a race
        // condition
        throw new AlreadyExistsException("%s already exists: %s", capitalize(type()), name());
      } else {
        throw new CommitFailedException("Cannot commit to %s %s: stale metadata", type(), name());
      }
    }
    // TODO: Should move this part into a background thread to enable adding potentially more
    // expensive computations
    // in this step, so that transactions are not held up.
    additionalAsyncMcps(datasetUrn, metadata, icebergBatch.getAuditStamp());
  }

  protected abstract DatasetProfile getDataSetProfile(M metadata);

  protected abstract StringMap getIcebergProperties(M metadata);

  FileIO io() {
    return io;
  }

  String name() {
    return fullTableName(platformInstance(), tableIdentifier);
  }

  private String platformInstance() {
    return warehouse.getPlatformInstance();
  }

  // override-able for testing
  @VisibleForTesting
  IcebergBatch newIcebergBatch(OperationContext operationContext) {
    return new IcebergBatch(operationContext);
  }

  abstract boolean isView();

  abstract boolean isExpectedType(boolean view);

  abstract M readMetadata(FileIO io, String location);

  abstract String type();

  abstract RuntimeException noSuchEntityException();

  // Any additional MCPs that need to be stored in the same transaction.
  void additionalMcps(M metadata, IcebergBatch.EntityBatch datasetBatch) {}

  // Useful properties in the metadata. They vary for table vs view.
  abstract void addMetadataProperties(
      DatasetPropertiesPatchBuilder patchBuilder, @Nonnull M metadata);

  // Any additional MCPs that can be ingested asynchronously (and hence outside the iceberg commit
  // path)
  void additionalAsyncMcps(
      DatasetUrn datasetUrn, MetadataWrapper<M> metadata, AuditStamp auditStamp) {
    MetadataChangeProposal datasetProfileMcp =
        getDatasetProfileMcp(datasetUrn, metadata, auditStamp);
    entityService.ingestProposal(operationContext, datasetProfileMcp, auditStamp, true);

    MetadataChangeProposal datasetPropertiesMcp =
        getDatasetProperties(datasetUrn, metadata, auditStamp);
    entityService.ingestProposal(operationContext, datasetPropertiesMcp, auditStamp, true);
  }

  private MetadataChangeProposal getDatasetProfileMcp(
      DatasetUrn datasetUrn, MetadataWrapper<M> metadata, AuditStamp auditStamp) {
    DatasetProfile datasetProfile =
        getDataSetProfile(metadata.metadata()).setTimestampMillis(auditStamp.getTime());

    MetadataChangeProposal datasetProfileMcp =
        new MetadataChangeProposal()
            .setEntityUrn(datasetUrn)
            .setEntityType(DATASET_ENTITY_NAME)
            .setAspectName(DATASET_PROFILE_ASPECT_NAME)
            .setAspect(serializeAspect(datasetProfile))
            .setChangeType(ChangeType.UPSERT);
    return datasetProfileMcp;
  }

  private MetadataChangeProposal getDatasetProperties(
      DatasetUrn datasetUrn, MetadataWrapper<M> metadata, AuditStamp auditStamp) {
    StringMap newIcebergProperties = getIcebergProperties(metadata.metadata());
    Set<String> deletedIcebergProperties = null;
    RecordTemplate prevDatasetPropertiesData =
        entityService.getLatestAspect(operationContext, datasetUrn, DATASET_PROPERTIES_ASPECT_NAME);
    if (prevDatasetPropertiesData != null) {
      DatasetProperties prevPropertiesAspect =
          new DatasetProperties(prevDatasetPropertiesData.data());
      StringMap prevPropertiesMap = prevPropertiesAspect.getCustomProperties();
      Set<String> prevIcebergProperties =
          prevPropertiesMap.keySet().stream()
              .filter(key -> key.startsWith(ICEBERG_PROPERTY_PREFIX))
              .collect(Collectors.toSet());

      deletedIcebergProperties =
          prevIcebergProperties.stream()
              .filter(key -> !newIcebergProperties.containsKey(key))
              .collect(Collectors.toSet());
    }
    DatasetPropertiesPatchBuilder patchBuilder =
        new DatasetPropertiesPatchBuilder().urn(datasetUrn);
    if (deletedIcebergProperties != null) {
      for (String deletedProperty : deletedIcebergProperties) {
        patchBuilder.removeCustomProperty(deletedProperty);
      }
    }

    // Some useful metadata fields added as properties.
    // Properties vary for tables and views, hence subclass implements them.
    addMetadataProperties(patchBuilder, metadata.metadata());

    // TBLPROPERTIES set via DDL have 'iceberg:' prefix
    for (String newProperty : newIcebergProperties.keySet()) {
      patchBuilder.addCustomProperty(
          ICEBERG_PROPERTY_PREFIX + newProperty, newIcebergProperties.get(newProperty));
    }
    return patchBuilder.build();
  }
}

@Slf4j
class ViewOpsDelegate extends TableOrViewOpsDelegate<ViewMetadata> {

  ViewOpsDelegate(
      DataHubIcebergWarehouse warehouse,
      TableIdentifier tableIdentifier,
      EntityService entityService,
      OperationContext operationContext,
      FileIOFactory fileIOFactory) {
    super(warehouse, tableIdentifier, entityService, operationContext, fileIOFactory);
  }

  @Override
  boolean isView() {
    return true;
  }

  @Override
  boolean isExpectedType(boolean view) {
    return view;
  }

  @Override
  ViewMetadata readMetadata(FileIO io, String location) {
    return ViewMetadataParser.read(io.newInputFile(location));
  }

  @Override
  String type() {
    return "view";
  }

  @Override
  RuntimeException noSuchEntityException() {
    return new NoSuchViewException("No such view %s", name());
  }

  @Override
  void additionalMcps(ViewMetadata metadata, IcebergBatch.EntityBatch datasetBatch) {
    SQLViewRepresentation sqlViewRepresentation = null;
    for (ViewRepresentation representation : metadata.currentVersion().representations()) {
      if (representation instanceof SQLViewRepresentation) {
        sqlViewRepresentation = (SQLViewRepresentation) representation;
        // use only first representation, as DataHub model currently supports one SQL.
        break;
      }
    }
    if (sqlViewRepresentation == null) {
      // base class is ensuring that a representation has been specified in case of replace-view.
      // so, this shouldn't occur.
      log.warn("No SQL representation for view {}", name());
    } else {
      ViewProperties viewProperties =
          new ViewProperties()
              .setViewLogic(sqlViewRepresentation.sql())
              .setMaterialized(false)
              .setViewLanguage(sqlViewRepresentation.dialect());
      datasetBatch.aspect(VIEW_PROPERTIES_ASPECT_NAME, viewProperties);
    }
  }

  @Override
  protected DatasetProfile getDataSetProfile(ViewMetadata metadata) {
    long columnCount = metadata.schema().columns().size();
    DatasetProfile datasetProfile = new DatasetProfile();
    datasetProfile.setColumnCount(columnCount);
    return datasetProfile;
  }

  @Override
  protected StringMap getIcebergProperties(ViewMetadata metadata) {
    return new StringMap(metadata.properties());
  }

  @Override
  void addMetadataProperties(
      DatasetPropertiesPatchBuilder patchBuilder, @Nonnull ViewMetadata metadata) {
    patchBuilder.addCustomProperty("location", metadata.location());
    patchBuilder.addCustomProperty("format-version", String.valueOf(metadata.formatVersion()));
    patchBuilder.addCustomProperty("view-uuid", metadata.uuid());
  }
}

class TableOpsDelegate extends TableOrViewOpsDelegate<TableMetadata> {

  TableOpsDelegate(
      DataHubIcebergWarehouse warehouse,
      TableIdentifier tableIdentifier,
      EntityService entityService,
      OperationContext operationContext,
      FileIOFactory fileIOFactory) {
    super(warehouse, tableIdentifier, entityService, operationContext, fileIOFactory);
  }

  @Override
  protected DatasetProfile getDataSetProfile(TableMetadata metadata) {

    DatasetProfile dataSetProfile = new DatasetProfile();
    long colCount = metadata.schema().columns().size();
    dataSetProfile.setColumnCount(colCount);

    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (currentSnapshot != null && currentSnapshot.summary() != null) {
      String totalRecordsStr = currentSnapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP);
      if (totalRecordsStr != null) {
        dataSetProfile.setRowCount(Long.parseLong(totalRecordsStr));
      }
      String totalFileSizeStr = currentSnapshot.summary().get(SnapshotSummary.TOTAL_FILE_SIZE_PROP);
      if (totalFileSizeStr != null) {
        dataSetProfile.setSizeInBytes(Long.parseLong(totalFileSizeStr));
      }
    }

    return dataSetProfile;
  }

  @Override
  boolean isView() {
    return false;
  }

  @Override
  boolean isExpectedType(boolean view) {
    return !view;
  }

  @Override
  TableMetadata readMetadata(FileIO io, String location) {
    return TableMetadataParser.read(io, location);
  }

  @Override
  String type() {
    return "table";
  }

  @Override
  RuntimeException noSuchEntityException() {
    return new NoSuchTableException("No such table %s", name());
  }

  @Override
  protected StringMap getIcebergProperties(TableMetadata metadata) {
    return new StringMap(metadata.properties());
  }

  @Override
  void addMetadataProperties(
      DatasetPropertiesPatchBuilder patchBuilder, @Nonnull TableMetadata metadata) {
    patchBuilder.addCustomProperty("location", metadata.location());
    patchBuilder.addCustomProperty("format-version", String.valueOf(metadata.formatVersion()));
    patchBuilder.addCustomProperty("table-uuid", metadata.uuid());
    if (metadata.currentSnapshot() != null) {
      patchBuilder.addCustomProperty(
          "snapshot-id", String.valueOf(metadata.currentSnapshot().snapshotId()));
      patchBuilder.addCustomProperty(
          "sequence-number", String.valueOf(metadata.currentSnapshot().sequenceNumber()));
      patchBuilder.addCustomProperty(
          "manifest-list", metadata.currentSnapshot().manifestListLocation());
    }
  }
}

class MetadataWrapper<M> {
  final TableMetadata tableMetadata;
  final ViewMetadata viewMetadata;
  final boolean view;

  MetadataWrapper(TableMetadata metadata) {
    this.tableMetadata = metadata;
    viewMetadata = null;
    view = false;
  }

  MetadataWrapper(ViewMetadata metadata) {
    this.viewMetadata = metadata;
    tableMetadata = null;
    view = true;
  }

  int currentSchemaId() {
    if (view) {
      return viewMetadata.currentSchemaId();
    } else {
      return tableMetadata.currentSchemaId();
    }
  }

  org.apache.iceberg.Schema schema() {
    if (view) {
      return viewMetadata.schema();
    } else {
      return tableMetadata.schema();
    }
  }

  String location() {
    if (view) {
      return viewMetadata.location();
    } else {
      return tableMetadata.location();
    }
  }

  String metadataFileLocation() {
    if (view) {
      return viewMetadata.metadataFileLocation();
    } else {
      return tableMetadata.metadataFileLocation();
    }
  }

  M metadata() {
    if (view) {
      return (M) viewMetadata;
    } else {
      return (M) tableMetadata;
    }
  }
}
