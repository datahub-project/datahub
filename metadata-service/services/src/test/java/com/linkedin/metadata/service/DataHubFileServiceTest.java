package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.file.DataHubFileInfo;
import com.linkedin.file.FileUploadScenario;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataHubFileKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import io.datahubproject.metadata.context.OperationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubFileServiceTest {
  private EntityClient mockEntityClient;
  private DataHubFileService service;
  private OperationContext mockOpContext;
  private Urn fileUrn;
  private DataHubFileKey key;

  @BeforeMethod
  public void setup() throws Exception {
    mockEntityClient = mock(EntityClient.class);
    service = new DataHubFileService(mockEntityClient);
    mockOpContext = mock(OperationContext.class);
    key = new DataHubFileKey().setId("test-file-id");
    fileUrn = EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATAHUB_FILE_ENTITY_NAME);
    when(mockOpContext.getAuditStamp()).thenReturn(new AuditStamp());
  }

  @Test
  public void testCreateDataHubFileSuccess() throws Exception {
    when(mockEntityClient.exists(any(), any())).thenReturn(false);
    when(mockEntityClient.batchIngestProposals(any(), anyList(), eq(false))).thenReturn(null);

    Urn urn =
        service.createDataHubFile(
            mockOpContext,
            "test-file-id",
            "s3://bucket/path/file.png",
            "file.png",
            "image/png",
            1024L,
            FileUploadScenario.DOCUMENTATION,
            null,
            null);

    assertNotNull(urn);
    assertEquals(urn.toString(), fileUrn.toString());
    verify(mockEntityClient, times(1)).exists(any(), eq(fileUrn));
    verify(mockEntityClient, times(1)).batchIngestProposals(any(), anyList(), eq(false));
  }

  @Test
  public void testCreateDataHubFileWithReferences() throws Exception {
    when(mockEntityClient.exists(any(), any())).thenReturn(false);
    when(mockEntityClient.batchIngestProposals(any(), anyList(), eq(false))).thenReturn(null);

    Urn assetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    Urn schemaFieldUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:test,test,PROD),testField)");

    Urn urn =
        service.createDataHubFile(
            mockOpContext,
            "test-file-id",
            "s3://bucket/path/file.png",
            "file.png",
            "image/png",
            1024L,
            FileUploadScenario.DOCUMENTATION,
            assetUrn,
            schemaFieldUrn);

    assertNotNull(urn);
    assertEquals(urn.toString(), fileUrn.toString());
    verify(mockEntityClient, times(1)).exists(any(), eq(fileUrn));
    verify(mockEntityClient, times(1)).batchIngestProposals(any(), anyList(), eq(false));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testCreateDataHubFileAlreadyExists() throws Exception {
    when(mockEntityClient.exists(any(), any())).thenReturn(true);

    service.createDataHubFile(
        mockOpContext,
        "test-file-id",
        "s3://bucket/path/file.png",
        "file.png",
        "image/png",
        1024L,
        FileUploadScenario.DOCUMENTATION,
        null,
        null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testCreateDataHubFileFailure() throws Exception {
    when(mockEntityClient.exists(any(), any())).thenReturn(false);
    doThrow(new RuntimeException("fail"))
        .when(mockEntityClient)
        .batchIngestProposals(any(), anyList(), eq(false));

    service.createDataHubFile(
        mockOpContext,
        "test-file-id",
        "s3://bucket/path/file.png",
        "file.png",
        "image/png",
        1024L,
        FileUploadScenario.DOCUMENTATION,
        null,
        null);
  }

  @Test
  public void testFileExistsTrue() throws Exception {
    when(mockEntityClient.exists(any(), eq(fileUrn))).thenReturn(true);

    boolean exists = service.fileExists(mockOpContext, fileUrn);

    assertTrue(exists);
    verify(mockEntityClient, times(1)).exists(any(), eq(fileUrn));
  }

  @Test
  public void testFileExistsFalse() throws Exception {
    when(mockEntityClient.exists(any(), eq(fileUrn))).thenReturn(false);

    boolean exists = service.fileExists(mockOpContext, fileUrn);

    assertFalse(exists);
    verify(mockEntityClient, times(1)).exists(any(), eq(fileUrn));
  }

  @Test
  public void testGetDataHubFileInfoFound() throws Exception {
    DataHubFileInfo fileInfo = new DataHubFileInfo();
    fileInfo.setStorageLocation("s3://bucket/path/file.png");
    fileInfo.setOriginalFileName("file.png");
    fileInfo.setMimeType("image/png");
    fileInfo.setSizeInBytes(1024L);
    fileInfo.setScenario(FileUploadScenario.DOCUMENTATION);
    fileInfo.setCreated(new AuditStamp());

    EntityResponse response = mock(EntityResponse.class);
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(fileInfo.data()));
    aspectMap.put(Constants.DATAHUB_FILE_INFO_ASPECT_NAME, aspect);
    when(response.getAspects()).thenReturn(aspectMap);
    when(mockEntityClient.getV2(any(), anyString(), any(), any(), eq(false))).thenReturn(response);

    DataHubFileInfo result = service.getDataHubFileInfo(mockOpContext, fileUrn);

    assertNotNull(result);
    assertEquals(result.getStorageLocation(), "s3://bucket/path/file.png");
    assertEquals(result.getOriginalFileName(), "file.png");
  }

  @Test
  public void testGetDataHubFileInfoNotFound() throws Exception {
    EntityResponse response = mock(EntityResponse.class);
    EnvelopedAspectMap emptyAspectMap = new EnvelopedAspectMap();
    when(response.getAspects()).thenReturn(emptyAspectMap);
    when(mockEntityClient.getV2(any(), anyString(), any(), any(), eq(false))).thenReturn(response);

    DataHubFileInfo result = service.getDataHubFileInfo(mockOpContext, fileUrn);

    assertNull(result);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetDataHubFileEntityResponseThrows() throws Exception {
    when(mockEntityClient.getV2(any(), anyString(), any(), any(), eq(false)))
        .thenThrow(new RuntimeException("fail"));

    service.getDataHubFileEntityResponse(mockOpContext, fileUrn);
  }

  @Test
  public void testDeleteDataHubFileSuccess() throws Exception {
    doNothing().when(mockEntityClient).deleteEntity(any(), any());

    service.deleteDataHubFile(mockOpContext, fileUrn);

    verify(mockEntityClient, times(1)).deleteEntity(mockOpContext, fileUrn);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testDeleteDataHubFileFailure() throws Exception {
    doThrow(new RuntimeException("Test exception"))
        .when(mockEntityClient)
        .deleteEntity(any(), any());

    service.deleteDataHubFile(mockOpContext, fileUrn);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testDeleteDataHubFileWithNullUrn() throws Exception {
    service.deleteDataHubFile(mockOpContext, null);
  }
}
