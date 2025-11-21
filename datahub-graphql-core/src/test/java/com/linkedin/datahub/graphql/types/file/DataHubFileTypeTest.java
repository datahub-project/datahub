package com.linkedin.datahub.graphql.types.file;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubFile;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.file.DataHubFileInfo;
import com.linkedin.file.FileUploadScenario;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubFileTypeTest {

  private DataHubFileType dataHubFileType;
  private EntityClient mockEntityClient;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setUp() {
    mockEntityClient = mock(EntityClient.class);
    mockQueryContext = getMockAllowContext();
    dataHubFileType = new DataHubFileType(mockEntityClient);
  }

  @Test
  public void testType() {
    assertEquals(dataHubFileType.type(), EntityType.DATAHUB_FILE);
  }

  @Test
  public void testObjectClass() {
    assertEquals(dataHubFileType.objectClass(), DataHubFile.class);
  }

  @Test
  public void testGetKeyProvider() {
    // Create a mock entity
    DataHubFile mockEntity = new DataHubFile();
    mockEntity.setUrn("urn:li:dataHubFile:test-file");

    String result = dataHubFileType.getKeyProvider().apply(mockEntity);
    assertEquals(result, "urn:li:dataHubFile:test-file");
  }

  @Test
  public void testBatchLoadSuccess() throws Exception {
    // Create test URNs
    String urn1 = "urn:li:dataHubFile:file-1";
    String urn2 = "urn:li:dataHubFile:file-2";
    List<String> urns = Arrays.asList(urn1, urn2);

    // Create mock entity responses
    EntityResponse response1 = createMockEntityResponse(urn1, "file1.png");
    EntityResponse response2 = createMockEntityResponse(urn2, "file2.jpg");

    Map<Urn, EntityResponse> entityMap = new HashMap<>();
    entityMap.put(UrnUtils.getUrn(urn1), response1);
    entityMap.put(UrnUtils.getUrn(urn2), response2);

    // Mock the entity client response
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubFile"), any(), eq(DataHubFileType.ASPECTS_TO_FETCH)))
        .thenReturn(entityMap);

    // Execute batch load
    List<DataFetcherResult<DataHubFile>> results =
        dataHubFileType.batchLoad(urns, mockQueryContext);

    // Verify results
    assertEquals(results.size(), 2);

    DataFetcherResult<DataHubFile> result1 = results.get(0);
    assertNotNull(result1);
    assertNotNull(result1.getData());
    assertEquals(result1.getData().getUrn(), urn1);
    assertEquals(result1.getData().getType(), EntityType.DATAHUB_FILE);
    assertEquals(result1.getData().getInfo().getOriginalFileName(), "file1.png");

    DataFetcherResult<DataHubFile> result2 = results.get(1);
    assertNotNull(result2);
    assertNotNull(result2.getData());
    assertEquals(result2.getData().getUrn(), urn2);
    assertEquals(result2.getData().getType(), EntityType.DATAHUB_FILE);
    assertEquals(result2.getData().getInfo().getOriginalFileName(), "file2.jpg");
  }

  @Test
  public void testBatchLoadWithMissingEntities() throws Exception {
    // Create test URNs
    String urn1 = "urn:li:dataHubFile:file-1";
    String urn2 = "urn:li:dataHubFile:file-2";
    List<String> urns = Arrays.asList(urn1, urn2);

    // Create mock entity response for only one entity
    EntityResponse response1 = createMockEntityResponse(urn1, "file1.png");

    Map<Urn, EntityResponse> entityMap = new HashMap<>();
    entityMap.put(UrnUtils.getUrn(urn1), response1);
    // Note: urn2 is missing from the map

    // Mock the entity client response
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubFile"), any(), eq(DataHubFileType.ASPECTS_TO_FETCH)))
        .thenReturn(entityMap);

    // Execute batch load
    List<DataFetcherResult<DataHubFile>> results =
        dataHubFileType.batchLoad(urns, mockQueryContext);

    // Verify results
    assertEquals(results.size(), 2);

    DataFetcherResult<DataHubFile> result1 = results.get(0);
    assertNotNull(result1);
    assertNotNull(result1.getData());
    assertEquals(result1.getData().getUrn(), urn1);

    DataFetcherResult<DataHubFile> result2 = results.get(1);
    assertNull(result2); // Missing entity should return null
  }

  @Test
  public void testBatchLoadEmptyList() throws Exception {
    List<String> urns = Collections.emptyList();

    // Mock the entity client response
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubFile"), any(), eq(DataHubFileType.ASPECTS_TO_FETCH)))
        .thenReturn(Collections.emptyMap());

    // Execute batch load
    List<DataFetcherResult<DataHubFile>> results =
        dataHubFileType.batchLoad(urns, mockQueryContext);

    // Verify results
    assertEquals(results.size(), 0);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testBatchLoadException() throws Exception {
    // Create test URNs
    String urn1 = "urn:li:dataHubFile:file-1";
    List<String> urns = Arrays.asList(urn1);

    // Mock the entity client to throw an exception
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubFile"), any(), eq(DataHubFileType.ASPECTS_TO_FETCH)))
        .thenThrow(new RemoteInvocationException("Test exception"));

    // Execute batch load - should throw RuntimeException
    dataHubFileType.batchLoad(urns, mockQueryContext);
  }

  @Test
  public void testAspectsToFetch() {
    assertEquals(DataHubFileType.ASPECTS_TO_FETCH.size(), 1);
    assertTrue(DataHubFileType.ASPECTS_TO_FETCH.contains("dataHubFileInfo"));
  }

  @Test
  public void testBatchLoadWithComplexFile() throws Exception {
    // Create test URNs
    String urn1 = "urn:li:dataHubFile:complex-file";
    List<String> urns = Arrays.asList(urn1);

    // Create mock entity response with complex file data
    EntityResponse response1 = createComplexMockEntityResponse(urn1);

    Map<Urn, EntityResponse> entityMap = new HashMap<>();
    entityMap.put(UrnUtils.getUrn(urn1), response1);

    // Mock the entity client response
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubFile"), any(), eq(DataHubFileType.ASPECTS_TO_FETCH)))
        .thenReturn(entityMap);

    // Execute batch load
    List<DataFetcherResult<DataHubFile>> results =
        dataHubFileType.batchLoad(urns, mockQueryContext);

    // Verify results
    assertEquals(results.size(), 1);

    DataFetcherResult<DataHubFile> result1 = results.get(0);
    assertNotNull(result1);
    assertNotNull(result1.getData());
    assertEquals(result1.getData().getUrn(), urn1);
    assertEquals(result1.getData().getType(), EntityType.DATAHUB_FILE);

    // Verify complex properties
    assertNotNull(result1.getData().getInfo());
    assertEquals(
        result1.getData().getInfo().getScenario(),
        com.linkedin.datahub.graphql.generated.UploadDownloadScenario.ASSET_DOCUMENTATION);
    assertNotNull(result1.getData().getInfo().getCreated());
    assertNotNull(result1.getData().getInfo().getReferencedByAsset());
    assertNotNull(result1.getData().getInfo().getSchemaField());
  }

  private EntityResponse createMockEntityResponse(String urn, String fileName) {
    // Create BucketStorageLocation
    com.linkedin.file.BucketStorageLocation location =
        new com.linkedin.file.BucketStorageLocation();
    location.setStorageBucket("my-bucket");
    location.setStorageKey("path/to/" + fileName);

    // Create GMS file info
    DataHubFileInfo gmsFileInfo = new DataHubFileInfo();
    gmsFileInfo.setBucketStorageLocation(location);
    gmsFileInfo.setOriginalFileName(fileName);
    gmsFileInfo.setMimeType("image/png");
    gmsFileInfo.setSizeInBytes(1024L);
    gmsFileInfo.setScenario(FileUploadScenario.ASSET_DOCUMENTATION);

    // Create audit stamp
    AuditStamp created = new AuditStamp();
    created.setTime(System.currentTimeMillis());
    created.setActor(UrnUtils.getUrn("urn:li:corpuser:test-user"));
    gmsFileInfo.setCreated(created);

    // Create entity response
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(urn));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(gmsFileInfo.data()));
    aspectMap.put("dataHubFileInfo", aspect);
    entityResponse.setAspects(aspectMap);

    return entityResponse;
  }

  private EntityResponse createComplexMockEntityResponse(String urn) {
    // Create BucketStorageLocation
    com.linkedin.file.BucketStorageLocation location =
        new com.linkedin.file.BucketStorageLocation();
    location.setStorageBucket("my-bucket");
    location.setStorageKey("path/to/complex-file.pdf");

    // Create GMS file info with references and optional fields
    DataHubFileInfo gmsFileInfo = new DataHubFileInfo();
    gmsFileInfo.setBucketStorageLocation(location);
    gmsFileInfo.setOriginalFileName("complex-file.pdf");
    gmsFileInfo.setMimeType("application/pdf");
    gmsFileInfo.setSizeInBytes(2048L);
    gmsFileInfo.setScenario(FileUploadScenario.ASSET_DOCUMENTATION);
    gmsFileInfo.setReferencedByAsset(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)"));
    gmsFileInfo.setSchemaField(
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:test,test,PROD),testField)"));
    gmsFileInfo.setContentHash("xyz789hash");

    // Create audit stamp
    AuditStamp created = new AuditStamp();
    created.setTime(System.currentTimeMillis());
    created.setActor(UrnUtils.getUrn("urn:li:corpuser:test-user"));
    gmsFileInfo.setCreated(created);

    // Create entity response
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(urn));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(gmsFileInfo.data()));
    aspectMap.put("dataHubFileInfo", aspect);
    entityResponse.setAspects(aspectMap);

    return entityResponse;
  }
}
