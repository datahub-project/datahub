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
import com.linkedin.dataset.ViewProperties;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.*;

@Slf4j
public class DataHubViewOps extends BaseViewOperations {

  private static final String DATASET_ICEBERG_METADATA_ASPECT_NAME = "icebergMetadata";

  private final String platformInstance;
  private FileIO io;
  private final TableIdentifier tableIdentifier;
  private final DatasetUrn urn;
  private final EntityService entityService;
  private final OperationContext operationContext;
  private final FileIOFactory fileIOFactory;
  private volatile ViewMetadata currentMetadata = null;
  private volatile boolean shouldRefresh = true;

  public DataHubViewOps(
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
  public ViewMetadata refresh() {
    IcebergMetadata icebergMeta =
        (IcebergMetadata)
            entityService.getLatestAspect(
                operationContext, urn, DATASET_ICEBERG_METADATA_ASPECT_NAME);
    if (icebergMeta == null || !icebergMeta.isView()) {
      return null;
    }
    String location = icebergMeta.getMetadataPointer();
    if (io == null) {
      String locationDir = location.substring(0, location.lastIndexOf("/"));
      io =
          fileIOFactory.createIO(
              platformInstance, PoliciesConfig.DATA_READ_ONLY_PRIVILEGE, Set.of(locationDir));
    }
    // TODO check UUID ala HadoopTableOps?
    currentMetadata = ViewMetadataParser.read(io().newInputFile(location));
    shouldRefresh = false;
    return currentMetadata;
  }

  @Override
  public ViewMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  @Override
  protected void doRefresh() {
    throw new UnsupportedOperationException();
  }

  @SneakyThrows
  @Override
  protected void doCommit(ViewMetadata base, ViewMetadata metadata) {

    EnvelopedAspect existingEnveloped =
        entityService.getLatestEnvelopedAspect(
            operationContext, DATASET_ENTITY_NAME, urn, DATASET_ICEBERG_METADATA_ASPECT_NAME);

    boolean creation = base == null;

    if (existingEnveloped != null) {
      if (creation) {
        throw new AlreadyExistsException("Table already exists: %s", viewName());
      }
      IcebergMetadata existingMetadata = new IcebergMetadata(existingEnveloped.getValue().data());
      if (!existingMetadata.isView()) {
        throw new NoSuchTableException("%s is not a view", viewName());
      }
      if (!existingMetadata.getMetadataPointer().equals(base.metadataFileLocation())) {
        throw new CommitFailedException(
            "Cannot commit to table %s: stale table metadata", viewName());
      }
    }

    // attempt to commit
    io =
        fileIOFactory.createIO(
            platformInstance,
            PoliciesConfig.DATA_READ_WRITE_PRIVILEGE,
            Set.of(metadata.location()));
    String newMetadataLocation = writeNewMetadataIfRequired(metadata);

    MetadataChangeProposal icebergMcp = newMcp(DATASET_ICEBERG_METADATA_ASPECT_NAME);
    icebergMcp.setAspect(
        serializeAspect(
            new IcebergMetadata().setMetadataPointer(newMetadataLocation).setView(true)));

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
        throw new AlreadyExistsException("View already exists: %s", viewName());
      } else {
        throw new CommitFailedException(
            "Cannot commit to table %s: stale table metadata", viewName());
      }
    }

    if (base == null || (base.currentSchemaId() != metadata.currentSchemaId())) {
      // schema changed
      Schema avroSchema = AvroSchemaUtil.convert(metadata.schema(), viewName());
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
      datasetProperties.setQualifiedName(viewName());

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
      log.warn("No SQL representation for view {}", viewName());
    } else {
      ViewProperties viewProperties =
          new ViewProperties()
              .setViewLogic(sqlViewRepresentation.sql())
              .setMaterialized(false)
              .setViewLanguage(sqlViewRepresentation.dialect());
      MetadataChangeProposal viewPropertiesMcp = newMcp(VIEW_PROPERTIES_ASPECT_NAME);
      viewPropertiesMcp.setAspect(serializeAspect(viewProperties));
      viewPropertiesMcp.setChangeType(ChangeType.UPSERT);

      ingestMcp(viewPropertiesMcp, auditStamp);
    }
  }

  @Override
  protected String viewName() {
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
