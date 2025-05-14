package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static io.datahubproject.iceberg.catalog.Utils.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.FabricType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatforminstance.IcebergWarehouseInfo;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.IcebergCatalogInfo;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.search.client.CacheEvictionService;
import com.linkedin.platformresource.PlatformResourceInfo;
import com.linkedin.secret.DataHubSecretValue;
import com.linkedin.util.Pair;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.net.URISyntaxException;
import java.util.*;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.*;

@Slf4j
public class DataHubIcebergWarehouse {

  public static final String DATASET_ICEBERG_METADATA_ASPECT_NAME = "icebergCatalogInfo";
  public static final String DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME =
      "icebergWarehouseInfo";

  private final EntityService entityService;

  private final SecretService secretService;

  private final OperationContext operationContext;

  private final IcebergWarehouseInfo icebergWarehouse;

  @Getter private final String platformInstance;

  private final CacheEvictionService cacheEvictionService;

  // When evicting a iceberg entity urn, these are additional urns that need to be evicted since
  // they are a way to
  // ge to the newly modified iceberg entity
  private final List<Urn> commonUrnsToEvict;

  @VisibleForTesting
  DataHubIcebergWarehouse(
      String platformInstance,
      IcebergWarehouseInfo icebergWarehouse,
      EntityService entityService,
      SecretService secretService,
      CacheEvictionService cacheEvictionService,
      OperationContext operationContext) {
    this.platformInstance = platformInstance;
    this.icebergWarehouse = icebergWarehouse;
    this.entityService = entityService;
    this.secretService = secretService;
    this.cacheEvictionService = cacheEvictionService;
    this.operationContext = operationContext;

    commonUrnsToEvict = List.of(Utils.platformInstanceUrn(platformInstance), Utils.platformUrn());
  }

  public static DataHubIcebergWarehouse of(
      String platformInstance,
      EntityService entityService,
      SecretService secretService,
      CacheEvictionService cacheEvictionService,
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
        platformInstance,
        icebergWarehouse,
        entityService,
        secretService,
        cacheEvictionService,
        operationContext);
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

  @SneakyThrows
  public Optional<DatasetUrn> getDatasetUrn(TableIdentifier tableIdentifier) {
    Urn resourceUrn = resourceUrn(tableIdentifier);
    Optional<PlatformResourceInfo> platformResourceInfo =
        getLatestAspectNonRemoved(resourceUrn, PLATFORM_RESOURCE_INFO_ASPECT_NAME);

    if (platformResourceInfo.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(DatasetUrn.createFromString(platformResourceInfo.get().getPrimaryKey()));
  }

  private <T extends RecordTemplate> Optional<T> getLatestAspectNonRemoved(
      Urn urn, String aspectName) {
    Map<Urn, List<RecordTemplate>> aspectsMap =
        entityService.getLatestAspects(
            operationContext, Set.of(urn), Set.of(STATUS_ASPECT_NAME, aspectName), false);

    if (aspectsMap == null || aspectsMap.isEmpty()) {
      return Optional.empty();
    }
    List<RecordTemplate> aspects = aspectsMap.get(urn);
    if (aspects == null || aspects.isEmpty()) {
      return Optional.empty();
    }

    T result = null;

    for (RecordTemplate aspect : aspects) {
      if (aspect instanceof Status status) {
        if (status.isRemoved()) {
          return Optional.empty();
        }
      } else {
        result = (T) aspect;
      }
    }

    return Optional.ofNullable(result);
  }

  private Optional<EnvelopedAspect> getLatestEnvelopedAspectNonRemoved(Urn urn, String aspectName)
      throws URISyntaxException {

    Map<Urn, List<EnvelopedAspect>> aspectsMap =
        entityService.getLatestEnvelopedAspects(
            operationContext, Set.of(urn), Set.of(STATUS_ASPECT_NAME, aspectName), false);

    if (aspectsMap == null || aspectsMap.isEmpty()) {
      return Optional.empty();
    }
    List<EnvelopedAspect> aspects = aspectsMap.get(urn);
    if (aspects == null || aspects.isEmpty()) {
      return Optional.empty();
    }

    EnvelopedAspect result = null;

    for (EnvelopedAspect aspect : aspects) {
      if (STATUS_ASPECT_NAME.equals(aspect.getName())) {
        Status status = new Status(aspect.getValue().data());
        if (status.isRemoved()) {
          return Optional.empty();
        }
      } else {
        result = aspect;
      }
    }

    return Optional.ofNullable(result);
  }

  public Optional<IcebergCatalogInfo> getIcebergMetadata(TableIdentifier tableIdentifier) {
    Optional<DatasetUrn> datasetUrn = getDatasetUrn(tableIdentifier);
    if (datasetUrn.isEmpty()) {
      return Optional.empty();
    }

    Optional<IcebergCatalogInfo> icebergMeta =
        getLatestAspectNonRemoved(datasetUrn.get(), DATASET_ICEBERG_METADATA_ASPECT_NAME);

    if (icebergMeta.isEmpty()) {
      // possibly some deletion cleanup is pending; log error & return as if dataset doesn't exist.
      log.error(
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
      Optional<EnvelopedAspect> existingEnveloped =
          getLatestEnvelopedAspectNonRemoved(
              datasetUrn.get(), DATASET_ICEBERG_METADATA_ASPECT_NAME);
      if (existingEnveloped.isEmpty()) {
        // possibly some deletion cleanup is pending; log error & return as if dataset doesn't
        // exist.
        log.error(
            String.format(
                "IcebergMetadata not found for resource %s, dataset %s",
                resourceUrn(tableIdentifier), datasetUrn.get()));
        return null;
      }
      return Pair.of(existingEnveloped.get(), datasetUrn.get());
    } catch (Exception e) {
      throw new RuntimeException(
          "Error fetching IcebergMetadata aspect for dataset " + datasetUrn.get(), e);
    }
  }

  public boolean deleteDataset(TableIdentifier tableIdentifier) {
    Urn resourceUrn = resourceUrn(tableIdentifier);
    if (!entityService.exists(operationContext, resourceUrn)) {
      return false;
    }

    Optional<DatasetUrn> datasetUrn = getDatasetUrn(tableIdentifier);
    if (datasetUrn.isEmpty()) {
      log.warn("Dataset urn not found for platform resource {}; cleaning up resource", resourceUrn);
      entityService.deleteUrn(operationContext, resourceUrn);
      return false;
    }

    IcebergBatch icebergBatch = newIcebergBatch(operationContext);
    icebergBatch.softDeleteEntity(resourceUrn, PLATFORM_RESOURCE_ENTITY_NAME);
    icebergBatch.softDeleteEntity(datasetUrn.get(), DATASET_ENTITY_NAME);

    AspectsBatch aspectsBatch = icebergBatch.asAspectsBatch();
    List<IngestResult> ingestResults =
        entityService.ingestProposal(operationContext, aspectsBatch, false);

    boolean result = true;
    for (IngestResult ingestResult : ingestResults) {
      if (ingestResult.getResult().isNoOp()) {
        result = false;
        break;
      }
    }

    entityService.deleteUrn(operationContext, resourceUrn);
    entityService.deleteUrn(operationContext, datasetUrn.get());
    invalidateCacheEntries(List.of(datasetUrn.get()));
    return result;
  }

  public DatasetUrn createDataset(
      TableIdentifier tableIdentifier, boolean view, IcebergBatch icebergBatch) {
    String datasetName = platformInstance + "." + UUID.randomUUID();
    DatasetUrn datasetUrn = new DatasetUrn(platformUrn(), datasetName, fabricType());

    createResource(datasetUrn, tableIdentifier, view, icebergBatch);

    Urn namespaceUrn = containerUrn(getPlatformInstance(), tableIdentifier.namespace());
    invalidateCacheEntries(List.of(datasetUrn, namespaceUrn));
    return datasetUrn;
  }

  void invalidateCacheEntries(List<Urn> urns) {
    ArrayList<Urn> urnsToEvict = new ArrayList<>(urns);
    urnsToEvict.addAll(commonUrnsToEvict);
    cacheEvictionService.evict(urnsToEvict);
  }

  public void renameDataset(TableIdentifier fromTableId, TableIdentifier toTableId, boolean view) {

    Optional<DatasetUrn> optDatasetUrn = getDatasetUrn(fromTableId);
    if (optDatasetUrn.isEmpty()) {
      throw noSuchEntity(view, fromTableId);
    }

    DatasetUrn datasetUrn = optDatasetUrn.get();

    IcebergBatch icebergBatch = newIcebergBatch(operationContext);
    icebergBatch.softDeleteEntity(resourceUrn(fromTableId), PLATFORM_RESOURCE_ENTITY_NAME);
    createResource(datasetUrn, toTableId, view, icebergBatch);

    DatasetProperties datasetProperties =
        new DatasetProperties()
            .setName(toTableId.name())
            .setQualifiedName(fullTableName(platformInstance, toTableId));

    IcebergBatch.EntityBatch datasetBatch =
        icebergBatch.updateEntity(datasetUrn, DATASET_ENTITY_NAME);
    datasetBatch.aspect(DATASET_PROPERTIES_ASPECT_NAME, datasetProperties);

    if (!fromTableId.namespace().equals(toTableId.namespace())) {
      Container container =
          new Container().setContainer(containerUrn(platformInstance, toTableId.namespace()));
      datasetBatch.aspect(CONTAINER_ASPECT_NAME, container);
    }

    try {
      AspectsBatch aspectsBatch = icebergBatch.asAspectsBatch();
      entityService.ingestProposal(operationContext, aspectsBatch, false);
    } catch (ValidationException e) {
      if (!entityService.exists(operationContext, resourceUrn(fromTableId), false)) {
        // someone else deleted "fromTable" before we could get through
        throw noSuchEntity(view, fromTableId);
      }
      if (entityService.exists(operationContext, resourceUrn(toTableId), true)) {
        throw new AlreadyExistsException(
            "%s already exists: %s",
            view ? "View" : "Table", fullTableName(platformInstance, toTableId));
      }
      throw new IllegalStateException(
          String.format(
              "Rename operation failed inexplicably, from %s to %s in warehouse %s",
              fromTableId, toTableId, platformInstance));
    }

    entityService.deleteUrn(operationContext, resourceUrn(fromTableId));

    Urn fromNamespaceUrn = containerUrn(getPlatformInstance(), fromTableId.namespace());

    List<Urn> urnsToInvalidate = new ArrayList<>(List.of(datasetUrn, fromNamespaceUrn));
    if (!fromTableId.namespace().equals(toTableId.namespace())) {
      Urn toNamespaceUrn = containerUrn(getPlatformInstance(), fromTableId.namespace());
      urnsToInvalidate.add(toNamespaceUrn);
    }
    invalidateCacheEntries(urnsToInvalidate);
  }

  private RuntimeException noSuchEntity(boolean view, TableIdentifier tableIdentifier) {
    return view
        ? new NoSuchViewException(
            "No such view %s", fullTableName(platformInstance, tableIdentifier))
        : new NoSuchTableException(
            "No such table %s", fullTableName(platformInstance, tableIdentifier));
  }

  private void createResource(
      DatasetUrn datasetUrn,
      TableIdentifier tableIdentifier,
      boolean view,
      IcebergBatch icebergBatch) {
    PlatformResourceInfo resourceInfo =
        new PlatformResourceInfo().setPrimaryKey(datasetUrn.toString());
    resourceInfo.setResourceType(view ? "icebergView" : "icebergTable");

    icebergBatch.createEntity(
        resourceUrn(tableIdentifier),
        PLATFORM_RESOURCE_ENTITY_NAME,
        PLATFORM_RESOURCE_INFO_ASPECT_NAME,
        resourceInfo);
  }

  private FabricType fabricType() {
    return icebergWarehouse.getEnv();
  }

  @SneakyThrows
  private Urn resourceUrn(TableIdentifier tableIdentifier) {
    return Urn.createFromString(
        String.format("urn:li:platformResource:%s.%s", PLATFORM_NAME, tableName(tableIdentifier)));
  }

  private String tableName(TableIdentifier tableIdentifier) {
    return fullTableName(platformInstance, tableIdentifier);
  }

  @VisibleForTesting
  IcebergBatch newIcebergBatch(OperationContext operationContext) {
    return new IcebergBatch(operationContext);
  }
}
