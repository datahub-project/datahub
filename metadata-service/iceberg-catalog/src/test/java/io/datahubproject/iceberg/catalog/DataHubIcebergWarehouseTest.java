package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse.DATASET_ICEBERG_METADATA_ASPECT_NAME;
import static io.datahubproject.iceberg.catalog.Utils.fullTableName;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.FabricType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatforminstance.IcebergWarehouseInfo;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.IcebergCatalogInfo;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.client.CacheEvictionService;
import com.linkedin.platformresource.PlatformResourceInfo;
import com.linkedin.secret.DataHubSecretValue;
import com.linkedin.util.Pair;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.util.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubIcebergWarehouseTest {

  @Mock private EntityService entityService;

  @Mock private SecretService secretService;

  @Mock private CacheEvictionService cacheEvictionService;

  @Mock private OperationContext operationContext;

  private IcebergWarehouseInfo icebergWarehouse;

  @Mock private RecordTemplate warehouseAspect;

  @Mock private IcebergBatch mockIcebergBatch;
  @Mock private AspectsBatch mockAspectsBatch;

  private final Urn testUser = new CorpuserUrn("urn:li:corpuser:testUser");

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    ActorContext actorContext = mock(ActorContext.class);
    when(operationContext.getActorContext()).thenReturn(actorContext);
    when(actorContext.getActorUrn()).thenReturn(testUser);

    when(mockIcebergBatch.asAspectsBatch()).thenReturn(mockAspectsBatch);
  }

  @Test
  public void testGetStorageProviderCredentials() throws Exception {
    String platformInstance = "test-platform";
    String clientId = "testClientId";
    String clientSecret = "testClientSecret";
    String role = "testRole";
    String dataRoot = "s3://data-root/test/";
    String region = "us-east-1";

    Urn clientIdUrn = Urn.createFromString("urn:li:secret:clientId");
    Urn clientSecretUrn = Urn.createFromString("urn:li:secret:clientSecret");

    icebergWarehouse = new IcebergWarehouseInfo();
    icebergWarehouse.setClientId(clientIdUrn);
    icebergWarehouse.setClientSecret(clientSecretUrn);
    icebergWarehouse.setDataRoot(dataRoot);
    icebergWarehouse.setRegion(region);
    icebergWarehouse.setRole(role);

    when(entityService.getLatestAspect(
            any(),
            any(),
            eq(DataHubIcebergWarehouse.DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME)))
        .thenReturn(warehouseAspect);
    when(warehouseAspect.data()).thenReturn(icebergWarehouse.data());

    when(secretService.decrypt(eq(clientId))).thenReturn("decrypted-" + clientId);
    when(secretService.decrypt(eq(clientSecret))).thenReturn("decrypted-" + clientSecret);

    DataHubSecretValue clientIdValue = new DataHubSecretValue();
    clientIdValue.setValue(clientId);

    DataHubSecretValue clientSecretValue = new DataHubSecretValue();
    clientSecretValue.setValue(clientSecret);

    Map<Urn, List<RecordTemplate>> aspectsMap = new HashMap<>();
    aspectsMap.put(clientIdUrn, Arrays.asList(clientIdValue));
    aspectsMap.put(clientSecretUrn, Arrays.asList(clientSecretValue));

    when(entityService.getLatestAspects(
            eq(operationContext),
            eq(Set.of(clientIdUrn, clientSecretUrn)),
            eq(Set.of("dataHubSecretValue")),
            eq(false)))
        .thenReturn(aspectsMap);

    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, cacheEvictionService, operationContext);

    CredentialProvider.StorageProviderCredentials credentials =
        warehouse.getStorageProviderCredentials();

    assertNotNull(credentials);
    assertEquals(credentials.clientId, "decrypted-" + clientId);
    assertEquals(credentials.clientSecret, "decrypted-" + clientSecret);
    assertEquals(credentials.role, role);
    assertEquals(credentials.region, region);
  }

  @Test(
      dependsOnMethods = {
        "testGetStorageProviderCredentials"
      }) // Dependency for icebergWarehouse setup
  public void testOf_Success() throws Exception {
    String platformInstance = "test-platform";
    when(entityService.getLatestAspect(
            any(OperationContext.class),
            any(Urn.class),
            eq(DataHubIcebergWarehouse.DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME)))
        .thenReturn(warehouseAspect);
    when(warehouseAspect.data()).thenReturn(icebergWarehouse.data());

    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, cacheEvictionService, operationContext);

    assertNotNull(warehouse);
    assertEquals(warehouse.getPlatformInstance(), platformInstance);
  }

  @Test(
      expectedExceptions = NotFoundException.class,
      expectedExceptionsMessageRegExp = "Unknown warehouse non-existent-platform")
  public void testOf_WarehouseNotFound() throws Exception {
    String platformInstance = "non-existent-platform";
    when(entityService.getLatestAspect(
            any(OperationContext.class),
            any(Urn.class),
            eq(DataHubIcebergWarehouse.DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME)))
        .thenReturn(null);

    DataHubIcebergWarehouse.of(
        platformInstance, entityService, secretService, cacheEvictionService, operationContext);
  }

  @Test
  public void testGetDataRoot() throws Exception {
    String platformInstance = "test-platform";
    String dataRoot = "s3://test-bucket/";

    when(entityService.getLatestAspect(
            any(),
            any(),
            eq(DataHubIcebergWarehouse.DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME)))
        .thenReturn(warehouseAspect);
    when(warehouseAspect.data())
        .thenReturn(new IcebergWarehouseInfo().setDataRoot(dataRoot).data());

    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, cacheEvictionService, operationContext);

    String result = warehouse.getDataRoot();

    assertEquals(result, dataRoot);
  }

  @Test
  public void testGetDatasetUrn() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier tableId = TableIdentifier.of("db", "table");
    Urn resourceUrn =
        Urn.createFromString("urn:li:platformResource:iceberg.test-platform.db.table");
    DatasetUrn expectedDatasetUrn =
        new DatasetUrn(
            DataPlatformUrn.createFromString("urn:li:dataPlatform:iceberg"),
            "uuid",
            FabricType.PROD);

    PlatformResourceInfo resourceInfo = new PlatformResourceInfo();
    resourceInfo.setPrimaryKey(expectedDatasetUrn.toString());

    when(entityService.getLatestAspects(
            same(operationContext),
            eq(Set.of(resourceUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, PLATFORM_RESOURCE_INFO_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Map.of(resourceUrn, List.of(new Status().setRemoved(false), resourceInfo)));

    DataHubIcebergWarehouse warehouse =
        new DataHubIcebergWarehouse(
            platformInstance,
            new IcebergWarehouseInfo(),
            entityService,
            secretService,
            cacheEvictionService,
            operationContext) {
          @Override
          IcebergBatch newIcebergBatch(OperationContext operationContext) {
            return mockIcebergBatch;
          }
        };

    Optional<DatasetUrn> result = warehouse.getDatasetUrn(tableId);

    assertTrue(result.isPresent());
    assertEquals(result.get(), expectedDatasetUrn);
  }

  @Test
  public void testGetIcebergMetadata() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier tableId = TableIdentifier.of("db", "table");
    Urn resourceUrn =
        Urn.createFromString("urn:li:platformResource:iceberg.test-platform.db.table");

    DatasetUrn datasetUrn =
        new DatasetUrn(
            DataPlatformUrn.createFromString("urn:li:dataPlatform:iceberg"),
            "uuid",
            FabricType.PROD);

    IcebergCatalogInfo expectedMetadata =
        new IcebergCatalogInfo().setMetadataPointer("s3://bucket/path");

    // Mock getDatasetUrn behavior
    PlatformResourceInfo resourceInfo = new PlatformResourceInfo();
    resourceInfo.setPrimaryKey(datasetUrn.toString());
    when(entityService.getLatestAspects(
            same(operationContext),
            eq(Set.of(resourceUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, PLATFORM_RESOURCE_INFO_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Map.of(resourceUrn, List.of(new Status().setRemoved(false), resourceInfo)));

    when(entityService.getLatestAspects(
            same(operationContext),
            eq(Set.of(datasetUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, DATASET_ICEBERG_METADATA_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Map.of(datasetUrn, List.of(expectedMetadata)));

    DataHubIcebergWarehouse warehouse =
        new DataHubIcebergWarehouse(
            platformInstance,
            new IcebergWarehouseInfo(),
            entityService,
            secretService,
            cacheEvictionService,
            operationContext) {
          @Override
          IcebergBatch newIcebergBatch(OperationContext operationContext) {
            return mockIcebergBatch;
          }
        };

    Optional<IcebergCatalogInfo> result = warehouse.getIcebergMetadata(tableId);

    assertTrue(result.isPresent());
    assertEquals(result.get().getMetadataPointer(), expectedMetadata.getMetadataPointer());
  }

  @Test
  public void testGetIcebergMetadataStatusRemoved() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier tableId = TableIdentifier.of("db", "table");
    Urn resourceUrn =
        Urn.createFromString("urn:li:platformResource:iceberg.test-platform.db.table");

    DatasetUrn datasetUrn =
        new DatasetUrn(
            DataPlatformUrn.createFromString("urn:li:dataPlatform:iceberg"),
            "uuid",
            FabricType.PROD);

    IcebergCatalogInfo expectedMetadata =
        new IcebergCatalogInfo().setMetadataPointer("s3://bucket/path");

    // Mock getDatasetUrn behavior
    PlatformResourceInfo resourceInfo = new PlatformResourceInfo();
    resourceInfo.setPrimaryKey(datasetUrn.toString());
    when(entityService.getLatestAspects(
            same(operationContext),
            eq(Set.of(resourceUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, PLATFORM_RESOURCE_INFO_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Map.of(resourceUrn, List.of(new Status().setRemoved(false), resourceInfo)));

    when(entityService.getLatestAspects(
            same(operationContext),
            eq(Set.of(datasetUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, DATASET_ICEBERG_METADATA_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Map.of(datasetUrn, List.of(new Status().setRemoved(true), expectedMetadata)));

    DataHubIcebergWarehouse warehouse =
        new DataHubIcebergWarehouse(
            platformInstance,
            new IcebergWarehouseInfo(),
            entityService,
            secretService,
            cacheEvictionService,
            operationContext) {
          @Override
          IcebergBatch newIcebergBatch(OperationContext operationContext) {
            return mockIcebergBatch;
          }
        };

    Optional<IcebergCatalogInfo> result = warehouse.getIcebergMetadata(tableId);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetIcebergMetadataEnveloped() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier tableId = TableIdentifier.of("db", "table");
    Urn resourceUrn =
        Urn.createFromString("urn:li:platformResource:iceberg.test-platform.db.table");

    DatasetUrn datasetUrn =
        new DatasetUrn(
            DataPlatformUrn.createFromString("urn:li:dataPlatform:iceberg"),
            "uuid",
            FabricType.PROD);

    EnvelopedAspect expectedEnvelopedAspect = mock(EnvelopedAspect.class);

    // Mock getDatasetUrn behavior
    PlatformResourceInfo resourceInfo = new PlatformResourceInfo();
    resourceInfo.setPrimaryKey(datasetUrn.toString());
    when(entityService.getLatestAspects(
            same(operationContext),
            eq(Set.of(resourceUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, PLATFORM_RESOURCE_INFO_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Map.of(resourceUrn, List.of(new Status().setRemoved(false), resourceInfo)));

    when(entityService.getLatestEnvelopedAspects(
            same(operationContext),
            eq(Set.of(datasetUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, DATASET_ICEBERG_METADATA_ASPECT_NAME)),
            eq(false)))
        .thenReturn(
            Map.of(
                datasetUrn,
                List.of(
                    new EnvelopedAspect()
                        .setName(STATUS_ASPECT_NAME)
                        .setValue(new Aspect(new Status().setRemoved(false).data())),
                    expectedEnvelopedAspect)));

    DataHubIcebergWarehouse warehouse =
        new DataHubIcebergWarehouse(
            platformInstance,
            new IcebergWarehouseInfo(),
            entityService,
            secretService,
            cacheEvictionService,
            operationContext) {
          @Override
          IcebergBatch newIcebergBatch(OperationContext operationContext) {
            return mockIcebergBatch;
          }
        };

    Pair<EnvelopedAspect, DatasetUrn> result = warehouse.getIcebergMetadataEnveloped(tableId);

    assertNotNull(result);
    assertEquals(result.getFirst(), expectedEnvelopedAspect);
    assertEquals(result.getSecond(), datasetUrn);
  }

  @Test
  public void testGetIcebergMetadataEnvelopedStatusRemoved() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier tableId = TableIdentifier.of("db", "table");
    Urn resourceUrn =
        Urn.createFromString("urn:li:platformResource:iceberg.test-platform.db.table");

    DatasetUrn datasetUrn =
        new DatasetUrn(
            DataPlatformUrn.createFromString("urn:li:dataPlatform:iceberg"),
            "uuid",
            FabricType.PROD);

    EnvelopedAspect expectedEnvelopedAspect = mock(EnvelopedAspect.class);

    // Mock getDatasetUrn behavior
    PlatformResourceInfo resourceInfo = new PlatformResourceInfo();
    resourceInfo.setPrimaryKey(datasetUrn.toString());
    when(entityService.getLatestAspects(
            same(operationContext),
            eq(Set.of(resourceUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, PLATFORM_RESOURCE_INFO_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Map.of(resourceUrn, List.of(new Status().setRemoved(false), resourceInfo)));

    when(entityService.getLatestEnvelopedAspects(
            same(operationContext),
            eq(Set.of(datasetUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, DATASET_ICEBERG_METADATA_ASPECT_NAME)),
            eq(false)))
        .thenReturn(
            Map.of(
                datasetUrn,
                List.of(
                    new EnvelopedAspect()
                        .setName(STATUS_ASPECT_NAME)
                        .setValue(new Aspect(new Status().setRemoved(true).data())),
                    expectedEnvelopedAspect)));

    DataHubIcebergWarehouse warehouse =
        new DataHubIcebergWarehouse(
            platformInstance,
            new IcebergWarehouseInfo(),
            entityService,
            secretService,
            cacheEvictionService,
            operationContext) {
          @Override
          IcebergBatch newIcebergBatch(OperationContext operationContext) {
            return mockIcebergBatch;
          }
        };

    Pair<EnvelopedAspect, DatasetUrn> result = warehouse.getIcebergMetadataEnveloped(tableId);

    assertNull(result);
  }

  @Test
  public void testDeleteDataset() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier tableId = TableIdentifier.of("db", "table");
    Urn resourceUrn =
        Urn.createFromString("urn:li:platformResource:iceberg.test-platform.db.table");
    DatasetUrn datasetUrn =
        new DatasetUrn(
            DataPlatformUrn.createFromString("urn:li:dataPlatform:iceberg"),
            "uuid",
            FabricType.PROD);

    when(entityService.exists(eq(operationContext), eq(resourceUrn))).thenReturn(true);

    // Mock getDatasetUrn behavior
    PlatformResourceInfo resourceInfo = new PlatformResourceInfo();
    resourceInfo.setPrimaryKey(datasetUrn.toString());
    when(entityService.getLatestAspects(
            same(operationContext),
            eq(Set.of(resourceUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, PLATFORM_RESOURCE_INFO_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Map.of(resourceUrn, List.of(new Status().setRemoved(false), resourceInfo)));

    when(entityService.ingestProposal(same(operationContext), same(mockAspectsBatch), eq(false)))
        .thenReturn(List.of());

    DataHubIcebergWarehouse warehouse =
        new DataHubIcebergWarehouse(
            platformInstance,
            new IcebergWarehouseInfo(),
            entityService,
            secretService,
            cacheEvictionService,
            operationContext) {
          @Override
          IcebergBatch newIcebergBatch(OperationContext operationContext) {
            return mockIcebergBatch;
          }
        };

    boolean result = warehouse.deleteDataset(tableId);
    assertTrue(result);

    verify(mockIcebergBatch).softDeleteEntity(eq(resourceUrn), eq(PLATFORM_RESOURCE_ENTITY_NAME));
    verify(mockIcebergBatch).softDeleteEntity(eq(datasetUrn), eq(DATASET_ENTITY_NAME));
    verify(entityService).ingestProposal(same(operationContext), same(mockAspectsBatch), eq(false));
  }

  @Test
  public void testCreateDataset() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier tableId = TableIdentifier.of("db", "table");

    DataHubIcebergWarehouse warehouse =
        new DataHubIcebergWarehouse(
            platformInstance,
            new IcebergWarehouseInfo().setEnv(FabricType.PROD),
            entityService,
            secretService,
            cacheEvictionService,
            operationContext) {
          @Override
          IcebergBatch newIcebergBatch(OperationContext operationContext) {
            return mockIcebergBatch;
          }
        };

    Urn resourceUrn =
        Urn.createFromString("urn:li:platformResource:iceberg.test-platform.db.table");
    IcebergBatch icebergBatch = mock(IcebergBatch.class);
    when(icebergBatch.createEntity(
            eq(resourceUrn),
            eq(PLATFORM_RESOURCE_ENTITY_NAME),
            eq(PLATFORM_RESOURCE_INFO_ASPECT_NAME),
            any(PlatformResourceInfo.class)))
        .thenReturn(null);

    DatasetUrn result = warehouse.createDataset(tableId, false, icebergBatch);

    assertNotNull(result);
    assertEquals(result.getPlatformEntity(), Urn.createFromString("urn:li:dataPlatform:iceberg"));
    assertEquals(result.getOriginEntity(), FabricType.PROD);
    assertTrue(result.getDatasetNameEntity().startsWith(platformInstance + "."));

    verify(icebergBatch)
        .createEntity(
            eq(resourceUrn),
            eq(PLATFORM_RESOURCE_ENTITY_NAME),
            eq(PLATFORM_RESOURCE_INFO_ASPECT_NAME),
            eq(
                new PlatformResourceInfo()
                    .setPrimaryKey(result.toString())
                    .setResourceType("icebergTable")));
  }

  @Test
  public void testRenameDataset() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier fromTableId = TableIdentifier.of("db", "oldTable");
    TableIdentifier toTableId = TableIdentifier.of("db", "newTable");
    Urn fromResourceUrn =
        Urn.createFromString("urn:li:platformResource:iceberg.test-platform.db.oldTable");
    Urn toResourceUrn =
        Urn.createFromString("urn:li:platformResource:iceberg.test-platform.db.newTable");
    DatasetUrn existingDatasetUrn =
        new DatasetUrn(
            DataPlatformUrn.createFromString("urn:li:dataPlatform:iceberg"),
            "test-dataset",
            FabricType.PROD);

    // Mock getDatasetUrn behavior for source table

    PlatformResourceInfo resourceInfo =
        new PlatformResourceInfo()
            .setPrimaryKey(existingDatasetUrn.toString())
            .setResourceType("icebergTable");

    when(entityService.getLatestAspects(
            same(operationContext),
            eq(Set.of(fromResourceUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, PLATFORM_RESOURCE_INFO_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Map.of(fromResourceUrn, List.of(new Status().setRemoved(false), resourceInfo)));

    DataHubIcebergWarehouse warehouse =
        new DataHubIcebergWarehouse(
            platformInstance,
            new IcebergWarehouseInfo(),
            entityService,
            secretService,
            cacheEvictionService,
            operationContext) {
          @Override
          IcebergBatch newIcebergBatch(OperationContext operationContext) {
            return mockIcebergBatch;
          }
        };

    IcebergBatch.EntityBatch datasetUpdateBatch = mock(IcebergBatch.EntityBatch.class);
    when(mockIcebergBatch.updateEntity(eq(existingDatasetUrn), eq(DATASET_ENTITY_NAME)))
        .thenReturn(datasetUpdateBatch);

    when(entityService.ingestProposal(same(operationContext), same(mockAspectsBatch), eq(false)))
        .thenReturn(List.of());

    warehouse.renameDataset(fromTableId, toTableId, false);

    verify(mockIcebergBatch)
        .softDeleteEntity(eq(fromResourceUrn), eq(PLATFORM_RESOURCE_ENTITY_NAME));
    verify(mockIcebergBatch)
        .createEntity(
            eq(toResourceUrn),
            eq(PLATFORM_RESOURCE_ENTITY_NAME),
            eq(PLATFORM_RESOURCE_INFO_ASPECT_NAME),
            eq(resourceInfo));
    verify(datasetUpdateBatch)
        .aspect(
            DATASET_PROPERTIES_ASPECT_NAME,
            new DatasetProperties()
                .setName(toTableId.name())
                .setQualifiedName(fullTableName(platformInstance, toTableId)));
    // no container aspect as rename is within same namespace

    verify(entityService).ingestProposal(same(operationContext), same(mockAspectsBatch), eq(false));
    verify(entityService).deleteUrn(eq(operationContext), eq(fromResourceUrn));
  }

  @Test(expectedExceptions = NoSuchTableException.class)
  public void testRenameDataset_SourceTableNotFound() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier fromTableId = TableIdentifier.of("db", "oldTable");
    TableIdentifier toTableId = TableIdentifier.of("db", "newTable");

    DataHubIcebergWarehouse warehouse =
        new DataHubIcebergWarehouse(
            platformInstance,
            new IcebergWarehouseInfo(),
            entityService,
            secretService,
            cacheEvictionService,
            operationContext) {
          @Override
          IcebergBatch newIcebergBatch(OperationContext operationContext) {
            return mockIcebergBatch;
          }
        };

    // by default mock to return null on entity-service calls, so dataset should not be found
    warehouse.renameDataset(fromTableId, toTableId, false);
  }
}
