package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;
import static io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse.DATASET_ICEBERG_METADATA_ASPECT_NAME;
import static io.datahubproject.iceberg.catalog.Utils.fullTableName;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataplatforminstance.IcebergWarehouseInfo;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.IcebergCatalogInfo;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.mxe.MetadataChangeProposal;
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
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubIcebergWarehouseTest {

  @Mock private EntityService entityService;

  @Mock private SecretService secretService;

  @Mock private OperationContext operationContext;

  private IcebergWarehouseInfo icebergWarehouse;

  @Mock private RecordTemplate warehouseAspect;

  private final Urn testUser = new CorpuserUrn("urn:li:corpuser:testUser");

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    ActorContext actorContext = mock(ActorContext.class);
    when(operationContext.getActorContext()).thenReturn(actorContext);
    when(actorContext.getActorUrn()).thenReturn(testUser);
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
            platformInstance, entityService, secretService, operationContext);

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
            platformInstance, entityService, secretService, operationContext);

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

    DataHubIcebergWarehouse.of(platformInstance, entityService, secretService, operationContext);
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
            platformInstance, entityService, secretService, operationContext);

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

    when(entityService.getLatestAspect(
            any(),
            any(),
            eq(DataHubIcebergWarehouse.DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME)))
        .thenReturn(warehouseAspect);
    when(warehouseAspect.data()).thenReturn(new IcebergWarehouseInfo().data());

    when(entityService.getLatestAspects(
            same(operationContext),
            eq(Set.of(resourceUrn)),
            eq(Set.of(STATUS_ASPECT_NAME, PLATFORM_RESOURCE_INFO_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Map.of(resourceUrn, List.of(new Status().setRemoved(false), resourceInfo)));

    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, operationContext);

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

    when(entityService.getLatestAspect(
            any(),
            any(),
            eq(DataHubIcebergWarehouse.DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME)))
        .thenReturn(warehouseAspect);
    when(warehouseAspect.data()).thenReturn(new IcebergWarehouseInfo().data());

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
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, operationContext);

    Optional<IcebergCatalogInfo> result = warehouse.getIcebergMetadata(tableId);

    assertTrue(result.isPresent());
    assertEquals(result.get().getMetadataPointer(), expectedMetadata.getMetadataPointer());
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

    when(entityService.getLatestAspect(
            any(),
            any(),
            eq(DataHubIcebergWarehouse.DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME)))
        .thenReturn(warehouseAspect);
    when(warehouseAspect.data()).thenReturn(new IcebergWarehouseInfo().data());

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
        .thenReturn(Map.of(datasetUrn, List.of(expectedEnvelopedAspect)));

    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, operationContext);

    Pair<EnvelopedAspect, DatasetUrn> result = warehouse.getIcebergMetadataEnveloped(tableId);

    assertNotNull(result);
    assertEquals(result.getFirst(), expectedEnvelopedAspect);
    assertEquals(result.getSecond(), datasetUrn);
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

    when(entityService.getLatestAspect(
            any(),
            any(),
            eq(DataHubIcebergWarehouse.DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME)))
        .thenReturn(warehouseAspect);
    when(warehouseAspect.data()).thenReturn(new IcebergWarehouseInfo().data());
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

    List<MetadataChangeProposal> expectedMcps = new ArrayList<>();
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(resourceUrn)
            .setEntityType(PLATFORM_RESOURCE_ENTITY_NAME)
            .setAspectName(STATUS_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Status().setRemoved(true)))
            .setChangeType(ChangeType.UPDATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(datasetUrn)
            .setEntityType(DATASET_ENTITY_NAME)
            .setAspectName(STATUS_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Status().setRemoved(true)))
            .setChangeType(ChangeType.UPDATE));

    AspectsBatchImpl markerBatch = mock(AspectsBatchImpl.class);

    when(entityService.ingestProposal(same(operationContext), same(markerBatch), eq(false)))
        .thenReturn(List.of());

    AspectsBatchImpl.AspectsBatchImplBuilder builder =
        mock(AspectsBatchImpl.AspectsBatchImplBuilder.class);
    when(builder.mcps(eq(expectedMcps), any(AuditStamp.class), any())).thenReturn(builder);
    when(builder.build()).thenReturn(markerBatch);

    try (MockedStatic<AspectsBatchImpl> stsClientMockedStatic =
        mockStatic(AspectsBatchImpl.class)) {
      stsClientMockedStatic.when(AspectsBatchImpl::builder).thenReturn(builder);

      DataHubIcebergWarehouse warehouse =
          DataHubIcebergWarehouse.of(
              platformInstance, entityService, secretService, operationContext);

      boolean result = warehouse.deleteDataset(tableId);
      assertTrue(result);
    }

    verify(builder).mcps(eq(expectedMcps), any(AuditStamp.class), any());
    verify(entityService).deleteUrn(eq(operationContext), eq(resourceUrn));
    verify(entityService).deleteUrn(eq(operationContext), eq(datasetUrn));
    verify(entityService).ingestProposal(same(operationContext), same(markerBatch), eq(false));
  }

  @Test
  public void testCreateDataset() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier tableId = TableIdentifier.of("db", "table");

    when(entityService.getLatestAspect(any(), any(), eq("icebergWarehouseInfo")))
        .thenReturn(warehouseAspect);
    IcebergWarehouseInfo warehouse = new IcebergWarehouseInfo().setEnv(FabricType.PROD);
    when(warehouseAspect.data()).thenReturn(warehouse.data());

    DataHubIcebergWarehouse icebergWarehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, operationContext);

    Urn resourceUrn =
        Urn.createFromString("urn:li:platformResource:iceberg.test-platform.db.table");
    IcebergBatch icebergBatch = mock(IcebergBatch.class);
    when(icebergBatch.createEntity(
            eq(resourceUrn),
            eq(PLATFORM_RESOURCE_ENTITY_NAME),
            eq(PLATFORM_RESOURCE_INFO_ASPECT_NAME),
            any(PlatformResourceInfo.class)))
        .thenReturn(null);

    DatasetUrn result = icebergWarehouse.createDataset(tableId, false, icebergBatch);

    assertNotNull(result);
    assertEquals(result.getPlatformEntity(), Urn.createFromString("urn:li:dataPlatform:iceberg"));
    assertEquals(result.getOriginEntity(), FabricType.PROD);
    assertTrue(result.getDatasetNameEntity().startsWith(platformInstance + "."));

    // TODO validate platformResourceInfo
    verify(icebergBatch)
        .createEntity(
            eq(resourceUrn),
            eq(PLATFORM_RESOURCE_ENTITY_NAME),
            eq(PLATFORM_RESOURCE_INFO_ASPECT_NAME),
            any(PlatformResourceInfo.class));
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

    when(entityService.getLatestAspect(
            any(),
            any(),
            eq(DataHubIcebergWarehouse.DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME)))
        .thenReturn(warehouseAspect);
    when(warehouseAspect.data()).thenReturn(new IcebergWarehouseInfo().data());

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
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, operationContext);

    List<MetadataChangeProposal> expectedMcps = new ArrayList<>();
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(fromResourceUrn)
            .setEntityType(PLATFORM_RESOURCE_ENTITY_NAME)
            .setAspectName(STATUS_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Status().setRemoved(true)))
            .setChangeType(ChangeType.UPDATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(toResourceUrn)
            .setEntityType(PLATFORM_RESOURCE_ENTITY_NAME)
            .setAspectName(PLATFORM_RESOURCE_INFO_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(resourceInfo))
            .setChangeType(ChangeType.CREATE_ENTITY));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(toResourceUrn)
            .setEntityType(PLATFORM_RESOURCE_ENTITY_NAME)
            .setAspectName(STATUS_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(serializeAspect(new Status().setRemoved(false)))
            .setChangeType(ChangeType.CREATE));
    expectedMcps.add(
        new MetadataChangeProposal()
            .setEntityUrn(existingDatasetUrn)
            .setEntityType(DATASET_ENTITY_NAME)
            .setAspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .setHeaders(
                new StringMap(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, Boolean.toString(true))))
            .setAspect(
                serializeAspect(
                    new DatasetProperties()
                        .setName(toTableId.name())
                        .setQualifiedName(fullTableName(platformInstance, toTableId))))
            .setChangeType(ChangeType.UPDATE));

    // no container aspect as rename is within same namespace
    AspectsBatchImpl markerBatch = mock(AspectsBatchImpl.class);

    when(entityService.ingestProposal(same(operationContext), same(markerBatch), eq(false)))
        .thenReturn(List.of());

    AspectsBatchImpl.AspectsBatchImplBuilder builder =
        mock(AspectsBatchImpl.AspectsBatchImplBuilder.class);

    when(builder.mcps(eq(expectedMcps), any(AuditStamp.class), any())).thenReturn(builder);
    when(builder.build()).thenReturn(markerBatch);

    try (MockedStatic<AspectsBatchImpl> stsClientMockedStatic =
        mockStatic(AspectsBatchImpl.class)) {
      stsClientMockedStatic.when(AspectsBatchImpl::builder).thenReturn(builder);
      warehouse.renameDataset(fromTableId, toTableId, false);
    }

    verify(builder).mcps(eq(expectedMcps), any(AuditStamp.class), any());
    verify(entityService).ingestProposal(same(operationContext), same(markerBatch), eq(false));
    verify(entityService).deleteUrn(eq(operationContext), eq(fromResourceUrn));
  }

  @Test(expectedExceptions = NoSuchTableException.class)
  public void testRenameDataset_SourceTableNotFound() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier fromTableId = TableIdentifier.of("db", "oldTable");
    TableIdentifier toTableId = TableIdentifier.of("db", "newTable");
    AuditStamp auditStamp =
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(Urn.createFromString("urn:li:corpuser:testUser"));

    when(entityService.getLatestAspect(any(), any(), eq("icebergWarehouseInfo")))
        .thenReturn(warehouseAspect);
    when(warehouseAspect.data()).thenReturn(new IcebergWarehouseInfo().data());

    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, operationContext);

    // by default mock to return null on entity-service calls, so dataset should not be found
    warehouse.renameDataset(fromTableId, toTableId, false);
  }
}
