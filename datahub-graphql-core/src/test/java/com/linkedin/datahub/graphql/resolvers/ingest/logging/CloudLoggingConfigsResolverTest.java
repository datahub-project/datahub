package com.linkedin.datahub.graphql.resolvers.ingest.logging;

import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.generated.CloudLoggingConfigsResolverResult;
import com.linkedin.metadata.config.ExecutorConfiguration;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CloudLoggingConfigsResolverTest {

  private ExecutorConfiguration createExecutorConfiguration(
      String bucket, String prefix, boolean loggingEnabled) {
    ExecutorConfiguration config = new ExecutorConfiguration();
    config.setCloudLoggingS3Bucket(bucket);
    config.setCloudLoggingS3Prefix(prefix);
    config.setRemoteExecutorLoggingEnabled(loggingEnabled);
    return config;
  }

  private CloudLoggingConfigsResolverResult executeResolver(ExecutorConfiguration config)
      throws Exception {
    CloudLoggingConfigsResolver resolver = new CloudLoggingConfigsResolver(config);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    return resolver.get(mockEnv).get();
  }

  private void assertResult(
      CloudLoggingConfigsResolverResult result,
      String expectedBucket,
      String expectedPrefix,
      boolean expectedLoggingEnabled) {
    assertNotNull(result);
    assertEquals(result.getS3_bucket(), expectedBucket);
    assertEquals(result.getS3_prefix(), expectedPrefix);
    assertEquals(result.getRemote_executor_logging_enabled(), expectedLoggingEnabled);
  }

  @Test
  public void testGetSuccess() throws Exception {
    ExecutorConfiguration config = createExecutorConfiguration("test-bucket", "test-prefix", true);
    CloudLoggingConfigsResolverResult result = executeResolver(config);
    assertResult(result, "test-bucket", "test-prefix", true);
  }

  @Test
  public void testGetWithNullBucket() throws Exception {
    ExecutorConfiguration config = createExecutorConfiguration(null, "test-prefix", false);
    CloudLoggingConfigsResolverResult result = executeResolver(config);
    assertResult(result, null, "test-prefix", false);
  }

  @Test
  public void testGetWithNullPrefix() throws Exception {
    ExecutorConfiguration config = createExecutorConfiguration("test-bucket", null, true);
    CloudLoggingConfigsResolverResult result = executeResolver(config);
    assertResult(result, "test-bucket", null, true);
  }

  @Test
  public void testGetWithEmptyStrings() throws Exception {
    ExecutorConfiguration config = createExecutorConfiguration("", "", false);
    CloudLoggingConfigsResolverResult result = executeResolver(config);
    assertResult(result, "", "", false);
  }

  @Test
  public void testGetWithDefaultConfiguration() throws Exception {
    ExecutorConfiguration config = new ExecutorConfiguration();
    CloudLoggingConfigsResolverResult result = executeResolver(config);
    assertResult(result, null, null, false);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testConstructorWithNullConfiguration() {
    new CloudLoggingConfigsResolver(null);
  }

  @Test
  public void testResultIsCompletedImmediately() throws Exception {
    ExecutorConfiguration config = createExecutorConfiguration("test-bucket", "test-prefix", true);
    CloudLoggingConfigsResolver resolver = new CloudLoggingConfigsResolver(config);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    CompletableFuture<CloudLoggingConfigsResolverResult> result = resolver.get(mockEnv);

    assertTrue(result.isDone());
    assertFalse(result.isCompletedExceptionally());
  }

  @Test
  public void testMultipleCallsReturnSameValues() throws Exception {
    ExecutorConfiguration config =
        createExecutorConfiguration("consistent-bucket", "consistent-prefix", true);
    CloudLoggingConfigsResolver resolver = new CloudLoggingConfigsResolver(config);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    CompletableFuture<CloudLoggingConfigsResolverResult> result1 = resolver.get(mockEnv);
    CompletableFuture<CloudLoggingConfigsResolverResult> result2 = resolver.get(mockEnv);

    CloudLoggingConfigsResolverResult actualResult1 = result1.get();
    CloudLoggingConfigsResolverResult actualResult2 = result2.get();

    assertEquals(actualResult1.getS3_bucket(), actualResult2.getS3_bucket());
    assertEquals(actualResult1.getS3_prefix(), actualResult2.getS3_prefix());
    assertEquals(
        actualResult1.getRemote_executor_logging_enabled(),
        actualResult2.getRemote_executor_logging_enabled());
  }
}
