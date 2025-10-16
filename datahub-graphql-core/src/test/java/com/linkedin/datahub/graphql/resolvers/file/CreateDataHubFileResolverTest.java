package com.linkedin.datahub.graphql.resolvers.file;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateDataHubFileInput;
import com.linkedin.datahub.graphql.generated.CreateDataHubFileResponse;
import com.linkedin.datahub.graphql.generated.UploadDownloadScenario;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.file.DataHubFileInfo;
import com.linkedin.file.FileUploadScenario;
import com.linkedin.metadata.service.DataHubFileService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.testng.annotations.Test;

public class CreateDataHubFileResolverTest {
  private static final String TEST_FILE_ID = "test-file-123";
  private static final String TEST_FILE_URN = "urn:li:dataHubFile:test-file-123";
  private static final String TEST_STORAGE_LOCATION = "s3://bucket/path/file.png";
  private static final String TEST_FILE_NAME = "file.png";
  private static final String TEST_MIME_TYPE = "image/png";
  private static final Long TEST_SIZE = 1024L;

  @Test
  public void testGetSuccess() throws Exception {
    DataHubFileService mockService = mock(DataHubFileService.class);
    CreateDataHubFileResolver resolver = new CreateDataHubFileResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    CreateDataHubFileInput input = new CreateDataHubFileInput();
    input.setId(TEST_FILE_ID);
    input.setStorageLocation(TEST_STORAGE_LOCATION);
    input.setOriginalFileName(TEST_FILE_NAME);
    input.setMimeType(TEST_MIME_TYPE);
    input.setSizeInBytes(TEST_SIZE);
    input.setScenario(UploadDownloadScenario.ASSET_DOCUMENTATION);

    Urn fileUrn = UrnUtils.getUrn(TEST_FILE_URN);
    when(mockService.createDataHubFile(
            any(),
            eq(TEST_FILE_ID),
            eq(TEST_STORAGE_LOCATION),
            eq(TEST_FILE_NAME),
            eq(TEST_MIME_TYPE),
            eq(TEST_SIZE),
            any(),
            eq(null),
            eq(null)))
        .thenReturn(fileUrn);

    // Mock EntityResponse with a valid aspect map
    EntityResponse mockResponse = mock(EntityResponse.class);
    DataHubFileInfo fileInfo = new DataHubFileInfo();
    fileInfo.setStorageLocation(TEST_STORAGE_LOCATION);
    fileInfo.setOriginalFileName(TEST_FILE_NAME);
    fileInfo.setMimeType(TEST_MIME_TYPE);
    fileInfo.setSizeInBytes(TEST_SIZE);
    fileInfo.setScenario(FileUploadScenario.ASSET_DOCUMENTATION);

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:test-user"));
    fileInfo.setCreated(auditStamp);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(fileInfo.data()));
    aspectMap.put(com.linkedin.metadata.Constants.DATAHUB_FILE_INFO_ASPECT_NAME, aspect);
    when(mockResponse.getAspects()).thenReturn(aspectMap);
    when(mockResponse.getUrn()).thenReturn(fileUrn);
    when(mockService.getDataHubFileEntityResponse(any(), eq(fileUrn))).thenReturn(mockResponse);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    CreateDataHubFileResponse response = resolver.get(mockEnv).join();

    assertNotNull(response);
    assertNotNull(response.getFile());
    assertEquals(response.getFile().getUrn(), TEST_FILE_URN);

    verify(mockService, times(1))
        .createDataHubFile(
            any(),
            eq(TEST_FILE_ID),
            eq(TEST_STORAGE_LOCATION),
            eq(TEST_FILE_NAME),
            eq(TEST_MIME_TYPE),
            eq(TEST_SIZE),
            any(),
            eq(null),
            eq(null));
    verify(mockService, times(1)).getDataHubFileEntityResponse(any(), eq(fileUrn));
  }

  @Test
  public void testGetSuccessWithReferences() throws Exception {
    DataHubFileService mockService = mock(DataHubFileService.class);
    CreateDataHubFileResolver resolver = new CreateDataHubFileResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    CreateDataHubFileInput input = new CreateDataHubFileInput();
    input.setId(TEST_FILE_ID);
    input.setStorageLocation(TEST_STORAGE_LOCATION);
    input.setOriginalFileName(TEST_FILE_NAME);
    input.setMimeType(TEST_MIME_TYPE);
    input.setSizeInBytes(TEST_SIZE);
    input.setScenario(UploadDownloadScenario.ASSET_DOCUMENTATION);
    input.setReferencedByAsset("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    input.setSchemaField(
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:test,test,PROD),testField)");

    Urn fileUrn = UrnUtils.getUrn(TEST_FILE_URN);
    Urn assetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    Urn schemaFieldUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:test,test,PROD),testField)");

    when(mockService.createDataHubFile(
            any(),
            eq(TEST_FILE_ID),
            eq(TEST_STORAGE_LOCATION),
            eq(TEST_FILE_NAME),
            eq(TEST_MIME_TYPE),
            eq(TEST_SIZE),
            any(),
            eq(assetUrn),
            eq(schemaFieldUrn)))
        .thenReturn(fileUrn);

    // Mock EntityResponse
    EntityResponse mockResponse = mock(EntityResponse.class);
    DataHubFileInfo fileInfo = new DataHubFileInfo();
    fileInfo.setStorageLocation(TEST_STORAGE_LOCATION);
    fileInfo.setOriginalFileName(TEST_FILE_NAME);
    fileInfo.setMimeType(TEST_MIME_TYPE);
    fileInfo.setSizeInBytes(TEST_SIZE);
    fileInfo.setScenario(FileUploadScenario.ASSET_DOCUMENTATION);
    fileInfo.setReferencedByAsset(assetUrn);
    fileInfo.setSchemaField(schemaFieldUrn);

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:test-user"));
    fileInfo.setCreated(auditStamp);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(fileInfo.data()));
    aspectMap.put(com.linkedin.metadata.Constants.DATAHUB_FILE_INFO_ASPECT_NAME, aspect);
    when(mockResponse.getAspects()).thenReturn(aspectMap);
    when(mockResponse.getUrn()).thenReturn(fileUrn);
    when(mockService.getDataHubFileEntityResponse(any(), eq(fileUrn))).thenReturn(mockResponse);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    CreateDataHubFileResponse response = resolver.get(mockEnv).join();

    assertNotNull(response);
    assertNotNull(response.getFile());
    assertEquals(response.getFile().getUrn(), TEST_FILE_URN);

    verify(mockService, times(1))
        .createDataHubFile(
            any(),
            eq(TEST_FILE_ID),
            any(),
            any(),
            any(),
            any(),
            any(),
            eq(assetUrn),
            eq(schemaFieldUrn));
    verify(mockService, times(1)).getDataHubFileEntityResponse(any(), eq(fileUrn));
  }

  @Test
  public void testGetThrowsException() throws Exception {
    DataHubFileService mockService = mock(DataHubFileService.class);
    CreateDataHubFileResolver resolver = new CreateDataHubFileResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    CreateDataHubFileInput input = new CreateDataHubFileInput();
    input.setId(TEST_FILE_ID);
    input.setStorageLocation(TEST_STORAGE_LOCATION);
    input.setOriginalFileName(TEST_FILE_NAME);
    input.setMimeType(TEST_MIME_TYPE);
    input.setSizeInBytes(TEST_SIZE);
    input.setScenario(UploadDownloadScenario.ASSET_DOCUMENTATION);

    when(mockService.createDataHubFile(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(new RuntimeException("File already exists"));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    verify(mockService, times(1))
        .createDataHubFile(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testGetWithNullOptionalFields() throws Exception {
    DataHubFileService mockService = mock(DataHubFileService.class);
    CreateDataHubFileResolver resolver = new CreateDataHubFileResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    CreateDataHubFileInput input = new CreateDataHubFileInput();
    input.setId(TEST_FILE_ID);
    input.setStorageLocation(TEST_STORAGE_LOCATION);
    input.setOriginalFileName(TEST_FILE_NAME);
    input.setMimeType(TEST_MIME_TYPE);
    input.setSizeInBytes(TEST_SIZE);
    input.setScenario(UploadDownloadScenario.ASSET_DOCUMENTATION);
    // Don't set optional fields

    Urn fileUrn = UrnUtils.getUrn(TEST_FILE_URN);
    when(mockService.createDataHubFile(
            any(),
            eq(TEST_FILE_ID),
            eq(TEST_STORAGE_LOCATION),
            eq(TEST_FILE_NAME),
            eq(TEST_MIME_TYPE),
            eq(TEST_SIZE),
            any(),
            eq(null),
            eq(null)))
        .thenReturn(fileUrn);

    // Mock EntityResponse
    EntityResponse mockResponse = mock(EntityResponse.class);
    DataHubFileInfo fileInfo = new DataHubFileInfo();
    fileInfo.setStorageLocation(TEST_STORAGE_LOCATION);
    fileInfo.setOriginalFileName(TEST_FILE_NAME);
    fileInfo.setMimeType(TEST_MIME_TYPE);
    fileInfo.setSizeInBytes(TEST_SIZE);
    fileInfo.setScenario(FileUploadScenario.ASSET_DOCUMENTATION);

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:test-user"));
    fileInfo.setCreated(auditStamp);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(fileInfo.data()));
    aspectMap.put(com.linkedin.metadata.Constants.DATAHUB_FILE_INFO_ASPECT_NAME, aspect);
    when(mockResponse.getAspects()).thenReturn(aspectMap);
    when(mockResponse.getUrn()).thenReturn(fileUrn);
    when(mockService.getDataHubFileEntityResponse(any(), eq(fileUrn))).thenReturn(mockResponse);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    CreateDataHubFileResponse response = resolver.get(mockEnv).join();

    assertNotNull(response);
    assertNotNull(response.getFile());

    verify(mockService, times(1))
        .createDataHubFile(any(), any(), any(), any(), any(), any(), any(), eq(null), eq(null));
  }
}
