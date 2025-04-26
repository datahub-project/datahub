package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;
import static io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse.DATASET_ICEBERG_METADATA_ASPECT_NAME;
import static io.datahubproject.iceberg.catalog.Utils.*;
import static org.apache.commons.lang3.StringUtils.capitalize;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.container.Container;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.IcebergCatalogInfo;
import com.linkedin.dataset.ViewProperties;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.schematron.converters.avro.AvroSchemaConverter;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.*;
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

    DatasetProfile datasetProfile =
        getDataSetProfile(metadata.metadata())
            .setTimestampMillis(icebergBatch.getAuditStamp().getTime());

    MetadataChangeProposal datasetProfileMcp =
        new MetadataChangeProposal()
            .setEntityUrn(datasetUrn)
            .setEntityType(DATASET_ENTITY_NAME)
            .setAspectName(DATASET_PROFILE_ASPECT_NAME)
            .setAspect(serializeAspect(datasetProfile))
            .setChangeType(ChangeType.UPSERT);
    entityService.ingestProposal(
        operationContext, datasetProfileMcp, icebergBatch.getAuditStamp(), true);
  }

  protected abstract DatasetProfile getDataSetProfile(M metadata);

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

  void additionalMcps(M metadata, IcebergBatch.EntityBatch datasetBatch) {}
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
