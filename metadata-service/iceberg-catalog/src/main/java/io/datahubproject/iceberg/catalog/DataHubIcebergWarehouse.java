package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;
import static io.datahubproject.iceberg.catalog.Utils.*;

import com.google.common.util.concurrent.Striped;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatforminstance.IcebergWarehouseInfo;
import com.linkedin.dataset.IcebergCatalogInfo;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.platformresource.PlatformResourceInfo;
import com.linkedin.secret.DataHubSecretValue;
import com.linkedin.util.Pair;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.*;

public class DataHubIcebergWarehouse {

  public static final String DATASET_ICEBERG_METADATA_ASPECT_NAME = "icebergCatalogInfo";
  public static final String DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME =
      "icebergWarehouseInfo";

  private final EntityService entityService;

  private final SecretService secretService;

  private final OperationContext operationContext;

  private final IcebergWarehouseInfo icebergWarehouse;

  @Getter private final String platformInstance;

  // TODO: Need to handle locks for deployments with multiple GMS replicas.
  private static final Striped<Lock> resourceLocks =
      Striped.lazyWeakLock(Runtime.getRuntime().availableProcessors() * 2);

  private DataHubIcebergWarehouse(
      String platformInstance,
      IcebergWarehouseInfo icebergWarehouse,
      EntityService entityService,
      SecretService secretService,
      OperationContext operationContext) {
    this.platformInstance = platformInstance;
    this.icebergWarehouse = icebergWarehouse;
    this.entityService = entityService;
    this.secretService = secretService;
    this.operationContext = operationContext;
  }

  public static DataHubIcebergWarehouse of(
      String platformInstance,
      EntityService entityService,
      SecretService secretService,
      OperationContext operationContext) {
    Urn platformInstanceUrn = Utils.platformInstanceUrn(platformInstance);
    RecordTemplate warehouseAspect =
        entityService.getLatestAspect(
            operationContext,
            platformInstanceUrn,
            DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME);

    if (warehouseAspect == null) {
      throw new NotFoundException("Unknown warehouse " + platformInstance);
    }

    IcebergWarehouseInfo icebergWarehouse = new IcebergWarehouseInfo(warehouseAspect.data());
    return new DataHubIcebergWarehouse(
        platformInstance, icebergWarehouse, entityService, secretService, operationContext);
  }

  public CredentialProvider.StorageProviderCredentials getStorageProviderCredentials() {

    Urn clientIdUrn, clientSecretUrn;
    String role, region;
    Integer expirationSeconds;

    clientIdUrn = icebergWarehouse.getClientId();
    clientSecretUrn = icebergWarehouse.getClientSecret();
    role = icebergWarehouse.getRole();
    region = icebergWarehouse.getRegion();
    expirationSeconds = icebergWarehouse.getTempCredentialExpirationSeconds();

    Map<Urn, List<RecordTemplate>> credsMap =
        entityService.getLatestAspects(
            operationContext,
            Set.of(clientIdUrn, clientSecretUrn),
            Set.of("dataHubSecretValue"),
            false);

    DataHubSecretValue clientIdValue =
        new DataHubSecretValue(credsMap.get(clientIdUrn).get(0).data());

    String clientId = secretService.decrypt(clientIdValue.getValue());

    DataHubSecretValue clientSecretValue =
        new DataHubSecretValue(credsMap.get(clientSecretUrn).get(0).data());
    String clientSecret = secretService.decrypt(clientSecretValue.getValue());

    return new CredentialProvider.StorageProviderCredentials(
        clientId, clientSecret, role, region, expirationSeconds);
  }

  public String getDataRoot() {
    return icebergWarehouse.getDataRoot();
  }

  public Optional<DatasetUrn> getDatasetUrn(TableIdentifier tableIdentifier) {
    Urn resourceUrn = resourceUrn(tableIdentifier);
    PlatformResourceInfo platformResourceInfo =
        (PlatformResourceInfo)
            entityService.getLatestAspect(
                operationContext, resourceUrn, PLATFORM_RESOURCE_INFO_ASPECT_NAME);
    if (platformResourceInfo == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(DatasetUrn.createFromString(platformResourceInfo.getPrimaryKey()));
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid dataset urn " + platformResourceInfo.getPrimaryKey(), e);
    }
  }

  public IcebergCatalogInfo getIcebergMetadata(TableIdentifier tableIdentifier) {
    Optional<DatasetUrn> datasetUrn = getDatasetUrn(tableIdentifier);
    if (datasetUrn.isEmpty()) {
      return null;
    }

    IcebergCatalogInfo icebergMeta =
        (IcebergCatalogInfo)
            entityService.getLatestAspect(
                operationContext, datasetUrn.get(), DATASET_ICEBERG_METADATA_ASPECT_NAME);

    if (icebergMeta == null) {
      throw new IllegalStateException(
          String.format(
              "IcebergMetadata not found for resource %s, dataset %s",
              resourceUrn(tableIdentifier), datasetUrn.get()));
    }
    return icebergMeta;
  }

  public Pair<EnvelopedAspect, DatasetUrn> getIcebergMetadataEnveloped(
      TableIdentifier tableIdentifier) {
    Optional<DatasetUrn> datasetUrn = getDatasetUrn(tableIdentifier);
    if (datasetUrn.isEmpty()) {
      return null;
    }

    try {
      EnvelopedAspect existingEnveloped =
          entityService.getLatestEnvelopedAspect(
              operationContext,
              DATASET_ENTITY_NAME,
              datasetUrn.get(),
              DATASET_ICEBERG_METADATA_ASPECT_NAME);
      if (existingEnveloped == null) {
        throw new IllegalStateException(
            String.format(
                "IcebergMetadata not found for resource %s, dataset %s",
                resourceUrn(tableIdentifier), datasetUrn.get()));
      }
      return Pair.of(existingEnveloped, datasetUrn.get());
    } catch (Exception e) {
      throw new RuntimeException(
          "Error fetching IcebergMetadata aspect for dataset " + datasetUrn.get(), e);
    }
  }

  public boolean deleteDataset(TableIdentifier tableIdentifier) {
    Urn resourceUrn = resourceUrn(tableIdentifier);

    // guard against concurrent modifications that depend on the resource (rename table/view)
    Lock lock = resourceLocks.get(resourceUrn);
    lock.lock();
    try {
      if (!entityService.exists(operationContext, resourceUrn)) {
        return false;
      }
      Optional<DatasetUrn> urn = getDatasetUrn(tableIdentifier);
      entityService.deleteUrn(operationContext, resourceUrn);
      urn.ifPresent(x -> entityService.deleteUrn(operationContext, x));
      return true;
    } finally {
      lock.unlock();
    }
  }

  public DatasetUrn createDataset(
      TableIdentifier tableIdentifier, boolean view, AuditStamp auditStamp) {
    String datasetName = platformInstance + "." + UUID.randomUUID();
    DatasetUrn datasetUrn = new DatasetUrn(platformUrn(), datasetName, fabricType());
    createResource(datasetUrn, tableIdentifier, view, auditStamp);
    return datasetUrn;
  }

  public DatasetUrn renameDataset(
      TableIdentifier fromTableId, TableIdentifier toTableId, boolean view, AuditStamp auditStamp) {

    // guard against concurrent modifications to the resource (other renames, deletion)
    Lock lock = resourceLocks.get(resourceUrn(fromTableId));
    lock.lock();

    try {
      Optional<DatasetUrn> optDatasetUrn = getDatasetUrn(fromTableId);
      if (optDatasetUrn.isEmpty()) {
        if (view) {
          throw new NoSuchViewException(
              "No such view %s", fullTableName(platformInstance, fromTableId));
        } else {
          throw new NoSuchTableException(
              "No such table %s", fullTableName(platformInstance, fromTableId));
        }
      }

      DatasetUrn datasetUrn = optDatasetUrn.get();
      try {
        createResource(datasetUrn, toTableId, view, auditStamp);
      } catch (ValidationException e) {
        throw new AlreadyExistsException(
            "%s already exists: %s",
            view ? "View" : "Table", fullTableName(platformInstance, toTableId));
      }
      entityService.deleteUrn(operationContext, resourceUrn(fromTableId));
      return datasetUrn;
    } finally {
      lock.unlock();
    }
  }

  private void createResource(
      DatasetUrn datasetUrn, TableIdentifier tableIdentifier, boolean view, AuditStamp auditStamp) {
    PlatformResourceInfo resourceInfo =
        new PlatformResourceInfo().setPrimaryKey(datasetUrn.toString());
    resourceInfo.setResourceType(view ? "icebergView" : "icebergTable");

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(resourceUrn(tableIdentifier));
    mcp.setEntityType(PLATFORM_RESOURCE_ENTITY_NAME);
    mcp.setAspectName(PLATFORM_RESOURCE_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.CREATE_ENTITY);
    mcp.setAspect(serializeAspect(resourceInfo));

    entityService.ingestProposal(operationContext, mcp, auditStamp, false);
  }

  private FabricType fabricType() {
    return icebergWarehouse.getEnv();
  }

  @SneakyThrows
  private Urn resourceUrn(TableIdentifier tableIdentifier) {
    return Urn.createFromString(
        String.format(
            "urn:li:platformResource:%s.%s",
            PLATFORM_NAME, CatalogUtil.fullTableName(platformInstance, tableIdentifier)));
  }
}
