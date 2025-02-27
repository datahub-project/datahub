package io.datahubproject.iceberg.catalog;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatforminstance.IcebergWarehouseInfo;
import com.linkedin.dataset.IcebergCatalogInfo;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.platformresource.PlatformResourceInfo;
import com.linkedin.secret.DataHubSecretValue;
import com.linkedin.util.Pair;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

  @Mock private OperationContext operationContext;

  private IcebergWarehouseInfo icebergWarehouse;

  @Mock private RecordTemplate warehouseAspect;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
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
    when(entityService.getLatestAspect(
            eq(operationContext), eq(resourceUrn), eq("platformResourceInfo")))
        .thenReturn(resourceInfo);

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
    when(entityService.getLatestAspect(any(), any(), eq("platformResourceInfo")))
        .thenReturn(resourceInfo);

    when(entityService.getLatestAspect(
            eq(operationContext),
            eq(datasetUrn),
            eq(DataHubIcebergWarehouse.DATASET_ICEBERG_METADATA_ASPECT_NAME)))
        .thenReturn(expectedMetadata);

    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, operationContext);

    IcebergCatalogInfo result = warehouse.getIcebergMetadata(tableId);

    assertNotNull(result);
    assertEquals(result.getMetadataPointer(), expectedMetadata.getMetadataPointer());
  }

  @Test
  public void testGetIcebergMetadataEnveloped() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier tableId = TableIdentifier.of("db", "table");
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
    when(entityService.getLatestAspect(any(), any(), eq("platformResourceInfo")))
        .thenReturn(resourceInfo);

    when(entityService.getLatestEnvelopedAspect(
            eq(operationContext),
            eq("dataset"),
            eq(datasetUrn),
            eq(DataHubIcebergWarehouse.DATASET_ICEBERG_METADATA_ASPECT_NAME)))
        .thenReturn(expectedEnvelopedAspect);

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
    when(entityService.getLatestAspect(any(), any(), eq("platformResourceInfo")))
        .thenReturn(resourceInfo);

    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, operationContext);

    boolean result = warehouse.deleteDataset(tableId);

    assertTrue(result);
    verify(entityService).deleteUrn(eq(operationContext), eq(resourceUrn));
    verify(entityService).deleteUrn(eq(operationContext), eq(datasetUrn));
  }

  @Test
  public void testCreateDataset() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier tableId = TableIdentifier.of("db", "table");
    AuditStamp auditStamp =
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(Urn.createFromString("urn:li:corpuser:testUser"));

    when(entityService.getLatestAspect(any(), any(), eq("icebergWarehouseInfo")))
        .thenReturn(warehouseAspect);
    IcebergWarehouseInfo warehouse = new IcebergWarehouseInfo().setEnv(FabricType.PROD);
    when(warehouseAspect.data()).thenReturn(warehouse.data());

    DataHubIcebergWarehouse icebergWarehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, operationContext);

    DatasetUrn result = icebergWarehouse.createDataset(tableId, false, auditStamp);

    assertNotNull(result);
    assertEquals(result.getPlatformEntity(), Urn.createFromString("urn:li:dataPlatform:iceberg"));
    assertEquals(result.getOriginEntity(), FabricType.PROD);
    assertTrue(result.getDatasetNameEntity().startsWith(platformInstance + "."));

    verify(entityService)
        .ingestProposal(
            eq(operationContext), any(MetadataChangeProposal.class), eq(auditStamp), eq(false));
  }

  @Test
  public void testRenameDataset() throws Exception {
    String platformInstance = "test-platform";
    TableIdentifier fromTableId = TableIdentifier.of("db", "oldTable");
    TableIdentifier toTableId = TableIdentifier.of("db", "newTable");
    DatasetUrn existingDatasetUrn =
        new DatasetUrn(
            DataPlatformUrn.createFromString("urn:li:dataPlatform:iceberg"),
            "test-dataset",
            FabricType.PROD);
    AuditStamp auditStamp =
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(Urn.createFromString("urn:li:corpuser:testUser"));

    when(entityService.getLatestAspect(
            any(),
            any(),
            eq(DataHubIcebergWarehouse.DATAPLATFORM_INSTANCE_ICEBERG_WAREHOUSE_ASPECT_NAME)))
        .thenReturn(warehouseAspect);
    when(warehouseAspect.data()).thenReturn(new IcebergWarehouseInfo().data());

    // Mock getDatasetUrn behavior for source table
    PlatformResourceInfo resourceInfo = new PlatformResourceInfo();
    resourceInfo.setPrimaryKey(existingDatasetUrn.toString());
    when(entityService.getLatestAspect(any(), any(), eq("platformResourceInfo")))
        .thenReturn(resourceInfo);

    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, operationContext);

    DatasetUrn result = warehouse.renameDataset(fromTableId, toTableId, false, auditStamp);

    assertNotNull(result);
    assertEquals(result, existingDatasetUrn);

    verify(entityService)
        .ingestProposal(
            eq(operationContext), any(MetadataChangeProposal.class), eq(auditStamp), eq(false));
    verify(entityService).deleteUrn(eq(operationContext), any(Urn.class));
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

    // Mock empty response for getDatasetUrn
    when(entityService.getLatestAspect(any(), any(), eq("platformResourceInfo"))).thenReturn(null);

    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, operationContext);

    warehouse.renameDataset(fromTableId, toTableId, false, auditStamp);
  }
}
