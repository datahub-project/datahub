package io.datahubproject.openapi.v1.files;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.S3Configuration;
import com.linkedin.metadata.utils.aws.S3Util;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = FilesControllerTest.TestConfig.class)
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class FilesControllerTest extends AbstractTestNGSpringContextTests {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_FOLDER = "documents";
  private static final String TEST_FILE_ID = "abc123-def456-ghi789";
  private static final String TEST_PRESIGNED_URL =
      "https://s3.amazonaws.com/test-bucket/documents/abc123-def456-ghi789?signature=xyz";
  private static final int DEFAULT_EXPIRATION = 3600;
  private static final int MAX_EXPIRATION = 604800;

  @Autowired private FilesController filesController;

  @Autowired private MockMvc mockMvc;

  @Autowired private S3Util mockS3Util;

  @Autowired private ConfigurationProvider mockConfigProvider;

  @BeforeMethod
  public void setup() {
    // Setup default mock behavior for ConfigurationProvider
    DataHubConfiguration datahubConfig = mock(DataHubConfiguration.class);
    S3Configuration s3Config = mock(S3Configuration.class);

    when(mockConfigProvider.getDatahub()).thenReturn(datahubConfig);
    when(datahubConfig.getS3()).thenReturn(s3Config);
    when(s3Config.getBucketName()).thenReturn(TEST_BUCKET);
  }

  @Test
  public void initTest() {
    assertNotNull(filesController);
  }

  @Test
  public void testGetFileWithDefaultExpiration() throws Exception {
    // Mock S3Util to return presigned URL
    String expectedKey = String.format("%s/%s/%s", TEST_BUCKET, TEST_FOLDER, TEST_FILE_ID);
    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    // Test the endpoint
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/api/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithCustomExpiration() throws Exception {
    int customExpiration = 7200;
    String expectedKey = String.format("%s/%s/%s", TEST_BUCKET, TEST_FOLDER, TEST_FILE_ID);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(customExpiration)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/api/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
                .param("expiration", String.valueOf(customExpiration)))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithMinimumValidExpiration() throws Exception {
    int minExpiration = 1;
    String expectedKey = String.format("%s/%s/%s", TEST_BUCKET, TEST_FOLDER, TEST_FILE_ID);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(minExpiration)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/api/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
                .param("expiration", String.valueOf(minExpiration)))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithMaximumValidExpiration() throws Exception {
    String expectedKey = String.format("%s/%s/%s", TEST_BUCKET, TEST_FOLDER, TEST_FILE_ID);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(MAX_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/api/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
                .param("expiration", String.valueOf(MAX_EXPIRATION)))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithZeroExpiration() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/api/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
                .param("expiration", "0"))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testGetFileWithNegativeExpiration() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/api/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
                .param("expiration", "-100"))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testGetFileWithExcessiveExpiration() throws Exception {
    int excessiveExpiration = MAX_EXPIRATION + 1;

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/api/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID)
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
            MockMvcRequestBuilders.get("/api/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void testGetFileWithS3UtilException() throws Exception {
    String expectedKey = String.format("%s/%s/%s", TEST_BUCKET, TEST_FOLDER, TEST_FILE_ID);

    // Mock S3Util to throw an exception
    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenThrow(new RuntimeException("S3 service unavailable"));

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/api/files/{folder}/{fileId}", TEST_FOLDER, TEST_FILE_ID))
        .andExpect(status().isInternalServerError());
  }

  @Test
  public void testGetFileWithDifferentFolder() throws Exception {
    String differentFolder = "images";
    String expectedKey = String.format("%s/%s/%s", TEST_BUCKET, differentFolder, TEST_FILE_ID);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(
                "/api/files/{folder}/{fileId}", differentFolder, TEST_FILE_ID))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @Test
  public void testGetFileWithSpecialCharactersInFileId() throws Exception {
    String specialFileId = "file-with_special.chars-123";
    String expectedKey = String.format("%s/%s/%s", TEST_BUCKET, TEST_FOLDER, specialFileId);

    when(mockS3Util.generatePresignedDownloadUrl(
            eq(TEST_BUCKET), eq(expectedKey), eq(DEFAULT_EXPIRATION)))
        .thenReturn(TEST_PRESIGNED_URL);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/api/files/{folder}/{fileId}", TEST_FOLDER, specialFileId))
        .andExpect(status().isFound())
        .andExpect(header().string(HttpHeaders.LOCATION, TEST_PRESIGNED_URL));
  }

  @SpringBootConfiguration
  @Import({FilesControllerTestConfig.class})
  @ComponentScan(basePackages = {"com.datahub.files"})
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
  }
}
