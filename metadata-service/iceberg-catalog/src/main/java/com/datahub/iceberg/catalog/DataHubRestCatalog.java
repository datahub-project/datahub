package com.datahub.iceberg.catalog;

import static com.datahub.iceberg.catalog.Utils.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;

import com.datahub.iceberg.catalog.rest.DataHubIcebergWarehouse;
import com.google.common.base.Joiner;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
import org.apache.iceberg.view.ViewOperations;

public class DataHubRestCatalog extends BaseMetastoreViewCatalog implements SupportsNamespaces {
  private final CredentialProvider credentialProvider;

  private final EntityService entityService;

  private final OperationContext operationContext;

  private final CloseableGroup closeableGroup;

  private final String CATALOG_POINTER_ROOT_DIR = "s3://srinath-dev/icebreaker/";

  private final DataHubIcebergWarehouse warehouse;

  public DataHubRestCatalog(
      EntityService entityService,
      OperationContext operationContext,
      DataHubIcebergWarehouse warehouse,
      CredentialProvider credentialProvider) {
    this.entityService = entityService;
    this.operationContext = operationContext;
    this.credentialProvider = credentialProvider;
    this.warehouse = warehouse;
    this.closeableGroup = new CloseableGroup();
    this.closeableGroup.setSuppressCloseFailure(true);
  }

  @Override
  public void renameView(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {}

  @Override
  public void initialize(String name, Map<String, String> properties) {}

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new DataHubTableOps(
        platformInstance(),
        tableIdentifier,
        entityService,
        operationContext,
        new S3FileIOFactory());
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String warehouseRoot = warehouse.getDataRoot();
    return warehouseRoot
        + CatalogUtil.fullTableName(platformInstance(), tableIdentifier).replaceAll("\\.", "/");
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    if (purge) {
      throw new UnsupportedOperationException();
    }

    return deletaDataset(tableIdentifier);
  }

  private boolean deletaDataset(TableIdentifier tableIdentifier) {
    DatasetUrn urn = datasetUrn(platformInstance(), tableIdentifier);
    if (!entityService.exists(operationContext, urn)) {
      return false;
    }
    entityService.deleteUrn(operationContext, urn);
    return true;
  }

  @Override
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    if (tableExists(identifier)) {
      throw new AlreadyExistsException("Table already exists: %s", identifier);
    }

    FileIO io =
        new S3FileIOFactory()
            .createIO(
                platformInstance(),
                PoliciesConfig.DATA_READ_ONLY_PRIVILEGE,
                Set.of(parentDir(metadataFileLocation)));
    InputFile metadataFile = io.newInputFile(metadataFileLocation);
    TableMetadata metadata = TableMetadataParser.read(io, metadataFile);

    TableOperations ops = newTableOps(identifier);
    ops.commit(null, metadata);

    return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
  }

  @Override
  public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {
    // TODO
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> map) {
    AuditStamp auditStamp = auditStamp();
    Urn containerUrn = containerUrn(platformInstance(), namespace);

    int nLevels = namespace.length();
    if (nLevels > 1) {
      String[] parentLevels = Arrays.copyOfRange(namespace.levels(), 0, nLevels - 1);
      Urn parentContainerUrn = containerUrn(platformInstance(), parentLevels);
      if (!entityService.exists(operationContext, parentContainerUrn)) {
        throw new NoSuchNamespaceException(
            "Parent namespace %s does not exist in platformInstance-catalog %s",
            Joiner.on(".").join(parentLevels), platformInstance());
      }
      ingestContainerAspect(
          containerUrn,
          CONTAINER_ASPECT_NAME,
          new Container().setContainer(parentContainerUrn),
          auditStamp);
    }

    ingestContainerAspect(
        containerUrn,
        CONTAINER_PROPERTIES_ASPECT_NAME,
        new ContainerProperties().setName(namespace.levels()[nLevels - 1]),
        auditStamp);

    ingestContainerAspect(
        containerUrn,
        SUB_TYPES_ASPECT_NAME,
        new SubTypes().setTypeNames(new StringArray("IcebergNamespace")),
        auditStamp);

    MetadataChangeProposal platformInstanceMcp =
        platformInstanceMcp(platformInstance(), containerUrn, CONTAINER_ENTITY_NAME);
    ingestMcp(platformInstanceMcp, auditStamp);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return List.of();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    if (entityService.exists(operationContext, containerUrn(platformInstance(), namespace))) {
      return Map.of();
    } else {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    return false;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> map)
      throws NoSuchNamespaceException {
    return false;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> set)
      throws NoSuchNamespaceException {
    return false;
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.closeableGroup.close();
  }

  private void ingestContainerAspect(
      Urn containerUrn, String aspectName, RecordTemplate aspect, AuditStamp auditStamp) {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(containerUrn);
    mcp.setEntityType(CONTAINER_ENTITY_NAME);
    mcp.setAspectName(aspectName);
    mcp.setAspect(serializeAspect(aspect));
    mcp.setChangeType(ChangeType.UPSERT);
    ingestMcp(mcp, auditStamp);
  }

  private void ingestMcp(MetadataChangeProposal mcp, AuditStamp auditStamp) {
    entityService.ingestProposal(operationContext, mcp, auditStamp, false);
  }

  private class S3FileIOFactory implements FileIOFactory {
    @Override
    public FileIO createIO(
        String platformInstance, PoliciesConfig.Privilege privilege, Set<String> locations) {

      FileIO io = new S3FileIO();
      Map<String, String> creds =
          credentialProvider.get(
              new S3CredentialProvider.CredentialsCacheKey(platformInstance, privilege, locations),
              warehouse.getStorageProviderCredentials());
      io.initialize(creds);
      closeableGroup.addCloseable(io);
      return io;
    }

    @Override
    public FileIO createIO(
        String platformInstance, PoliciesConfig.Privilege privilege, TableMetadata tableMetadata) {
      return createIO(platformInstance, privilege, locations(tableMetadata));
    }
  }

  @Override
  protected ViewOperations newViewOps(TableIdentifier tableIdentifier) {
    return new DataHubViewOps(
        platformInstance(),
        tableIdentifier,
        entityService,
        operationContext,
        new S3FileIOFactory());
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    return List.of();
  }

  @Override
  public boolean dropView(TableIdentifier tableIdentifier) {
    return deletaDataset(tableIdentifier);
  }

  private String platformInstance() {
    return warehouse.getPlatformInstance();
  }
}
