package com.datahub.iceberg.catalog;

import static com.datahub.iceberg.catalog.Utils.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.container.Container;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.IcebergMetadata;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.schematron.converters.avro.AvroSchemaConverter;
import java.util.Collections;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;

public class DataHubTableOps extends BaseMetastoreTableOperations {

  private static final String DATASET_ICEBERG_METADATA_ASPECT_NAME = "icebergMetadata";

  private final String platformInstance;
  private FileIO io;
  private final TableIdentifier tableIdentifier;
  private final DatasetUrn urn;
  private final EntityService entityService;
  private final OperationContext operationContext;
  private final FileIOFactory fileIOFactory;
  private volatile TableMetadata currentMetadata = null;
  private volatile boolean shouldRefresh = true;

  public DataHubTableOps(
      String platformInstance,
      TableIdentifier tableIdentifier,
      EntityService entityService,
      OperationContext operationContext,
      FileIOFactory fileIOFactory) {
    this.platformInstance = platformInstance;
    this.tableIdentifier = tableIdentifier;
    this.entityService = entityService;
    this.operationContext = operationContext;
    this.fileIOFactory = fileIOFactory;
    this.urn = datasetUrn(platformInstance, tableIdentifier);
  }

  @Override
  public TableMetadata refresh() {
    IcebergMetadata icebergMeta =
        (IcebergMetadata)
            entityService.getLatestAspect(
                operationContext, urn, DATASET_ICEBERG_METADATA_ASPECT_NAME);
    if (icebergMeta == null || icebergMeta.isView()) {
      return null;
    }
    String location = icebergMeta.getMetadataPointer();
    if (io == null) {
      io =
          fileIOFactory.createIO(
              platformInstance,
              PoliciesConfig.DATA_READ_ONLY_PRIVILEGE,
              Set.of(parentDir(location)));
    }
    // TODO check UUID ala HadoopTableOps?
    currentMetadata = TableMetadataParser.read(io(), location);
    shouldRefresh = false;
    return currentMetadata;
  }

  @Override
  public TableMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  @SneakyThrows
  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {

    EnvelopedAspect existingEnveloped =
        entityService.getLatestEnvelopedAspect(
            operationContext, DATASET_ENTITY_NAME, urn, DATASET_ICEBERG_METADATA_ASPECT_NAME);

    boolean creation = base == null;

    if (existingEnveloped != null) {
      if (creation) {
        throw new AlreadyExistsException("Table already exists: %s", tableName());
      }
      IcebergMetadata existingMetadata = new IcebergMetadata(existingEnveloped.getValue().data());
      if (existingMetadata.isView()) {
        throw new NoSuchTableException("%s is not a table", tableName());
      }
      if (!existingMetadata.getMetadataPointer().equals(base.metadataFileLocation())) {
        throw new CommitFailedException(
            "Cannot commit to table %s: stale table metadata", tableName());
      }
    }

    // attempt to commit
    io =
        fileIOFactory.createIO(
            platformInstance, PoliciesConfig.DATA_READ_WRITE_PRIVILEGE, metadata);
    String newMetadataLocation = writeNewMetadataIfRequired(base == null, metadata);

    MetadataChangeProposal icebergMcp = newMcp(DATASET_ICEBERG_METADATA_ASPECT_NAME);
    icebergMcp.setAspect(
        serializeAspect(
            new IcebergMetadata().setMetadataPointer(newMetadataLocation).setView(false)));

    if (creation) {
      icebergMcp.setChangeType(ChangeType.CREATE_ENTITY);
    } else {
      String existingVersion = existingEnveloped.getSystemMetadata().getVersion();
      icebergMcp.setHeaders(
          new StringMap(Collections.singletonMap(HTTP_HEADER_IF_VERSION_MATCH, existingVersion)));
      icebergMcp.setChangeType(
          ChangeType.UPSERT); // ideally should be UPDATE, but seems not supported yet.
    }
    AuditStamp auditStamp = auditStamp();
    try {
      ingestMcp(icebergMcp, auditStamp);
    } catch (ValidationException e) {
      if (creation) {
        // this is likely because table already exists i.e. created concurrently in a race condition
        throw new AlreadyExistsException("Table already exists: %s", tableName());
      } else {
        throw new CommitFailedException(
            "Cannot commit to table %s: stale table metadata", tableName());
      }
    }

    if (base == null || (base.currentSchemaId() != metadata.currentSchemaId())) {
      // schema changed
      Schema avroSchema = AvroSchemaUtil.convert(metadata.schema(), tableName());
      AvroSchemaConverter converter = AvroSchemaConverter.builder().build();
      SchemaMetadata schemaMetadata =
          converter.toDataHubSchema(avroSchema, false, false, platformUrn(), null);
      MetadataChangeProposal schemaMcp = newMcp(SCHEMA_METADATA_ASPECT_NAME);
      schemaMcp.setAspect(serializeAspect(schemaMetadata));
      schemaMcp.setChangeType(ChangeType.UPSERT);
      ingestMcp(schemaMcp, auditStamp);
    }

    if (creation) {
      DatasetProperties datasetProperties = new DatasetProperties();
      datasetProperties.setName(tableIdentifier.name());
      datasetProperties.setQualifiedName(tableName());

      MetadataChangeProposal datasetPropertiesMcp = newMcp(DATASET_PROPERTIES_ASPECT_NAME);
      datasetPropertiesMcp.setAspect(serializeAspect(datasetProperties));
      datasetPropertiesMcp.setChangeType(ChangeType.UPSERT);

      ingestMcp(datasetPropertiesMcp, auditStamp);

      MetadataChangeProposal platformInstanceMcp =
          platformInstanceMcp(platformInstance, urn, DATASET_ENTITY_NAME);
      ingestMcp(platformInstanceMcp, auditStamp);

      Container container = new Container();
      container.setContainer(containerUrn(platformInstance, tableIdentifier.namespace()));

      MetadataChangeProposal containerMcp = newMcp(CONTAINER_ASPECT_NAME);
      containerMcp.setAspect(serializeAspect(container));
      containerMcp.setChangeType(ChangeType.UPSERT);
      ingestMcp(containerMcp, auditStamp);
    }
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    super.commit(base, metadata);
  }

  @Override
  protected String tableName() {
    return fullTableName(platformInstance, tableIdentifier);
  }

  @Override
  public FileIO io() {
    return io;
  }

  private MetadataChangeProposal newMcp(String aspectName) {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(DATASET_ENTITY_NAME);
    mcp.setAspectName(aspectName);
    return mcp;
  }

  private void ingestMcp(MetadataChangeProposal mcp, AuditStamp auditStamp) {
    entityService.ingestProposal(operationContext, mcp, auditStamp, false);
  }
}
