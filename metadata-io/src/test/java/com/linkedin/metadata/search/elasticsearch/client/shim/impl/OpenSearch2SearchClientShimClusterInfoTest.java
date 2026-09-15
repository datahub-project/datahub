package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.ShimConfigurationBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.ShimConfiguration;
import java.io.IOException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.core.rest.RestStatus;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class OpenSearch2SearchClientShimClusterInfoTest {

  private static RestHighLevelClient mockClientReturningForbidden() throws IOException {
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    when(mockClient.info(any(RequestOptions.class)))
        .thenThrow(
            new OpenSearchStatusException(
                "security_exception: no permissions for [cluster:monitor/main]",
                RestStatus.FORBIDDEN));
    return mockClient;
  }

  private static ListAppender<ILoggingEvent> captureShimLogs() {
    Logger shimLogger = (Logger) LoggerFactory.getLogger(OpenSearch2SearchClientShim.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    shimLogger.addAppender(appender);
    return appender;
  }

  private static boolean hasLog(ListAppender<ILoggingEvent> appender, Level level, String snippet) {
    return appender.list.stream()
        .anyMatch(
            event -> event.getLevel() == level && event.getFormattedMessage().contains(snippet));
  }

  @Test
  public void forbiddenClusterInfoLogsWarnWhenEngineTypeExplicit() throws Exception {
    ShimConfiguration config = new ShimConfigurationBuilder().build();
    assertFalse(config.isEngineTypeAutoDetected());
    OpenSearch2SearchClientShim shim =
        OpenSearch2SearchClientShim.forTest(mockClientReturningForbidden(), config);

    ListAppender<ILoggingEvent> appender = captureShimLogs();
    try {
      expectThrows(IOException.class, shim::getClusterInfo);
    } finally {
      ((Logger) LoggerFactory.getLogger(OpenSearch2SearchClientShim.class))
          .detachAppender(appender);
    }
    assertTrue(hasLog(appender, Level.WARN, "Cluster info API is restricted"));
    assertFalse(hasLog(appender, Level.ERROR, "Failed to get cluster info"));
  }

  @Test
  public void forbiddenClusterInfoLogsErrorWhenEngineTypeAutoDetected() throws Exception {
    ShimConfiguration config =
        new ShimConfigurationBuilder().withEngineTypeAutoDetected(true).build();
    assertTrue(new ShimConfigurationBuilder(config).build().isEngineTypeAutoDetected());
    OpenSearch2SearchClientShim shim =
        OpenSearch2SearchClientShim.forTest(mockClientReturningForbidden(), config);

    ListAppender<ILoggingEvent> appender = captureShimLogs();
    try {
      expectThrows(IOException.class, shim::getClusterInfo);
    } finally {
      ((Logger) LoggerFactory.getLogger(OpenSearch2SearchClientShim.class))
          .detachAppender(appender);
    }
    assertTrue(hasLog(appender, Level.ERROR, "Failed to get cluster info"));
    assertFalse(hasLog(appender, Level.WARN, "Cluster info API is restricted"));
  }
}
