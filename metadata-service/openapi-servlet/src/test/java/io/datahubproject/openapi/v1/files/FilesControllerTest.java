package io.datahubproject.openapi.v1.files;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.file.DataHubFileInfo;
import com.linkedin.file.FileUploadScenario;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.S3Configuration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.aws.S3Util;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = FilesControllerTest.TestConfig.class)
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class FilesControllerTest extends AbstractTestNGSpringContextTests {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_FOLDER = "documents";
  private static final String TEST_FILE_ID = "abc123-def456-ghi789";
  private static final String TEST_FILE_ID_WITH_SEPARATOR = "abc123__filename.pdf";
  private static final String TEST_PRESIGNED_URL =
      "https://s3.amazonaws.com/test-bucket/documents/abc123-def456-ghi789?signature=xyz";
  private static final int DEFAULT_EXPIRATION = 3600;
  private static final int MAX_EXPIRATION = 604800;
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_FILE_URN = UrnUtils.getUrn("urn:li:dataHubFile:abc123");
  private static final Urn TEST_ASSET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,my.dataset,PROD)");

  @Autowired private FilesController filesController;

  @Autowired private MockMvc mockMvc;

  @Autowired private S3Util mockS3Util;

  @Autowired private ConfigurationProvider mockConfigProvider;

  @Autowired private EntityService mockEntityService;

  @Autowired private OperationContext mockSystemOperationContext;

  @Autowired private AuthorizerChain mockAuthorizerChain;

  private MockedStatic<AuthenticationContext> authenticationContextMock;
  private MockedStatic<AuthUtil> authUtilMock;
  private Authentication mockAuthentication;

  @BeforeMethod
  public void setup() throws Exception {
    // Reset all mocks before each test
    Mockito.reset(mockS3Util, mockConfigProvider, mockEntityService);

    // Setup default mock behavior for ConfigurationProvider
    DataHubConfiguration datahubConfig = mock(DataHubConfiguration.class);
    S3Configuration s3Config = mock(S3Configuration.class);

    when(mockConfigProvider.getDatahub()).thenReturn(datahubConfig);
    when(datahubConfig.getS3()).thenReturn(s3Config);
    when(s3Config.getBucketName()).thenReturn(TEST_BUCKET);
    when(s3Config.getPresignedDownloadUrlExpirationSeconds()).thenReturn(DEFAULT_EXPIRATION);

    // Setup authentication mock
    mockAuthentication = mock(Authentication.class);
    Actor mockActor = new Actor(ActorType.USER, TEST_ACTOR_URN.getId());
    when(mockAuthentication.getActor()).thenReturn(mockActor);

    authenticationContextMock = Mockito.mockStatic(AuthenticationContext.class);
    authenticationContextMock
        .when(AuthenticationContext::getAuthentication)
        .thenReturn(mockAuthentication);

    // Setup AuthUtil mock - by default, allow access
    authUtilMock = Mockito.mockStatic(AuthUtil.class);
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(any(), any(), anySet()))
        .thenReturn(true);

    // Setup default EntityService behavior - return a valid file entity with ASSET_DOCUMENTATION
    // scenario
    setupDefaultFileEntity(TEST_FILE_URN, TEST_ASSET_URN, FileUploadScenario.ASSET_DOCUMENTATION);
  }

  @AfterMethod
  public void tearDown() {
    if (authenticationContextMock != null) {
      authenticationContextMock.close();
    }
    if (authUtilMock != null) {
      authUtilMock.close();
    }
  }

  private void setupDefaultFileEntity(Urn fileUrn, Urn assetUrn, FileUploadScenario scenario)
      throws Exception {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(fileUrn);
    entityResponse.setEntityName(Constants.DATAHUB_FILE_ENTITY_NAME);

    // Create DataHubFileInfo
    DataHubFileInfo fileInfo = new DataHubFileInfo();
    fileInfo.setScenario(scenario);
    fileInfo.setReferencedByAsset(assetUrn);

    // Wrap in EnvelopedAspect
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(fileInfo.data()));

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(Constants.DATAHUB_FILE_INFO_ASPECT_NAME, envelopedAspect);
    entityResponse.setAspects(new com.linkedin.entity.EnvelopedAspectMap(aspects));

    // Use any(Urn.class) to match any file URN since different tests may use different file IDs
    when(mockEntityService.getEntityV2(
            eq(mockSystemOperationContext),
            eq(Constants.DATAHUB_FILE_ENTITY_NAME),
            any(Urn.class),
            any(HashSet.class),
            anyBoolean()))
        .thenReturn(entityResponse);
  }

  @Test
  public void initTest() {
    assertNotNull(filesController);
  }

  @Test
  public void testGetFileWithDefaultExpiration() throws Exception {
    // Mock S3Util to return presigned URL
    String expectedKey = String.format("%s/%s", TEST_FOLDER, TEST_FILE_ID);
    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithCustomExpiration() throws Exception {
    int customExpiration = 7200;
    String expectedKey = String.format("%s/%s", TEST_FOLDER, TEST_FILE_ID);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(customExpiration)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
                .param("expiration", String.valueOf(customExpiration)))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithMinimumValidExpiration() throws Exception {
    int minExpiration = 1;
    String expectedKey = String.format("%s/%s", TEST_FOLDER, TEST_FILE_ID);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(minExpiration)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
                .param("expiration", String.valueOf(minExpiration)))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithMaximumValidExpiration() throws Exception {
    String expectedKey = String.format("%s/%s", TEST_FOLDER, TEST_FILE_ID);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(MAX_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
                .param("expiration", String.valueOf(MAX_EXPIRATION)))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithZeroExpiration() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
                .param("expiration", "0"))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testGetFileWithNegativeExpiration() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
                .param("expiration", "-100"))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testGetFileWithExcessiveExpiration() throws Exception {
    int excessiveExpiration = MAX_EXPIRATION + 1;

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                    "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
                .param("expiration", String.valueOf(excessiveExpiration)))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testGetFileWithNullBucket() throws Exception {
    // Override the mock to return null bucket
    DataHubConfiguration datahubConfig = mock(DataHubConfiguration.class);
    S3Configuration s3Config = mock(S3Configuration.class);

    when(mockConfigProvider.getDatahub()).thenReturn(datahubConfig);
    when(datahubConfig.getS3()).thenReturn(s3Config);
    when(s3Config.getBucketName()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testGetFileWithS3UtilException() throws Exception {
    String expectedKey = String.format("%s/%s", TEST_FOLDER, TEST_FILE_ID);

    // Mock S3Util to throw an exception
    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenThrow(new RuntimeException("S3 service unavailable"));

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isInternalServerError());
  }

  @Test
  public void testGetFileWithDifferentFolder() throws Exception {
    String differentFolder = "images";
    String expectedKey = String.format("%s/%s", differentFolder, TEST_FILE_ID);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", differentFolder, TEST_FILE_ID))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithSpecialCharactersInFileId() throws Exception {
    String specialFileId = "file-with_special.chars-123";
    String expectedKey = String.format("%s/%s", TEST_FOLDER, specialFileId);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, specialFileId))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  // ==================== Permission Validation Tests ====================

  @Test
  public void testGetFileWithValidAssetDocumentationPermissions() throws Exception {
    // Setup file ID with separator to test UUID extraction
    String fileIdWithSeparator = "abc123__filename.pdf";
    String expectedKey = String.format("%s/%s", TEST_FOLDER, fileIdWithSeparator);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    // AuthUtil should return true (already set up in setup())
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, fileIdWithSeparator))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithoutAssetDocumentationPermissions() throws Exception {
    // Override AuthUtil to deny access
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(any(), any(), anySet()))
        .thenReturn(false);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetFileWhenFileEntityDoesNotExist() throws Exception {
    // Override EntityService to return null
    when(mockEntityService.getEntityV2(
            eq(mockSystemOperationContext),
            eq(Constants.DATAHUB_FILE_ENTITY_NAME),
            any(Urn.class),
            any(HashSet.class),
            anyBoolean()))
        .thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetFileWhenFileEntityHasNoAspect() throws Exception {
    // Override EntityService to return entity without DataHubFileInfo aspect
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(TEST_FILE_URN);
    entityResponse.setEntityName(Constants.DATAHUB_FILE_ENTITY_NAME);
    entityResponse.setAspects(new com.linkedin.entity.EnvelopedAspectMap(new HashMap<>()));

    when(mockEntityService.getEntityV2(
            eq(mockSystemOperationContext),
            eq(Constants.DATAHUB_FILE_ENTITY_NAME),
            any(Urn.class),
            any(HashSet.class),
            anyBoolean()))
        .thenReturn(entityResponse);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testGetFileWithEntityServiceException() throws Exception {
    // Override EntityService to throw exception
    when(mockEntityService.getEntityV2(
            eq(mockSystemOperationContext),
            eq(Constants.DATAHUB_FILE_ENTITY_NAME),
            any(Urn.class),
            any(HashSet.class),
            anyBoolean()))
        .thenThrow(new RuntimeException("Database connection failed"));

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isInternalServerError());
  }

  @Test
  public void testGetFileWithUUIDExtraction() throws Exception {
    // Test that UUID is correctly extracted from file ID with separator
    String fileIdWithSeparator = "abc123__some-filename-with-special-chars.pdf";
    String expectedKey = String.format("%s/%s", TEST_FOLDER, fileIdWithSeparator);

    // The UUID "abc123" should be extracted
    Urn expectedFileUrn = UrnUtils.getUrn("urn:li:dataHubFile:abc123");

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, fileIdWithSeparator))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithDifferentAssetUrn() throws Exception {
    // Setup a different asset URN
    Urn differentAssetUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my.table,PROD)");
    setupDefaultFileEntity(
        TEST_FILE_URN, differentAssetUrn, FileUploadScenario.ASSET_DOCUMENTATION);

    String expectedKey = String.format("%s/%s", TEST_FOLDER, TEST_FILE_ID);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testBuildOperationContextWithCorrectParameters() throws Exception {
    // This test verifies that the endpoint successfully builds an operation context and processes
    // the request
    String expectedKey = String.format("%s/%s", TEST_FOLDER, TEST_FILE_ID);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/openapi/v1/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @SpringBootConfiguration
  @Import({
    FilesControllerTestConfig.class,
    io.datahubproject.openapi.config.GlobalControllerExceptionHandler.class
  })
  @ComponentScan(basePackages = {"io.datahubproject.openapi.v1.files"})
  static class TestConfig {}

  @TestConfiguration
  public static class FilesControllerTestConfig {

    @Bean
    @Primary
    @Qualifier("s3Util")
    public S3Util s3Util() {
      return mock(S3Util.class);
    }

    @Bean
    @Primary
    @Qualifier("configurationProvider")
    public ConfigurationProvider configurationProvider() {
      return mock(ConfigurationProvider.class);
    }

    @Bean
    @Primary
    @Qualifier("entityService")
    public EntityService entityService() {
      return mock(EntityService.class);
    }

    @Bean
    @Primary
    @Qualifier("systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @Bean
    @Primary
    @Qualifier("authorizerChain")
    public AuthorizerChain authorizerChain() {
      return mock(AuthorizerChain.class);
    }
  }
}
