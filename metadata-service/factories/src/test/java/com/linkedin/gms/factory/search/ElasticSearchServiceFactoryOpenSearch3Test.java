package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.linkedin.metadata.config.search.DocIdsConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityDocIdConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Covers the OpenSearch 3.x document-id startup diagnostic in {@link ElasticSearchServiceFactory}.
 */
public class ElasticSearchServiceFactoryOpenSearch3Test {

  private ListAppender<ILoggingEvent> appender;
  private Logger factoryLogger;
  private Level previousLevel;

  @BeforeMethod
  public void captureLogs() {
    factoryLogger = (Logger) LoggerFactory.getLogger(ElasticSearchServiceFactory.class);
    previousLevel = factoryLogger.getLevel();
    factoryLogger.setLevel(Level.WARN);
    appender = new ListAppender<>();
    appender.start();
    factoryLogger.addAppender(appender);
  }

  @AfterMethod
  public void releaseLogs() {
    factoryLogger.detachAppender(appender);
    factoryLogger.setLevel(previousLevel);
  }

  @Test
  public void warnsOnOpenSearch3WhenSchemaFieldHashingDisabled() {
    ElasticSearchServiceFactory.warnOpenSearch3DocIdRisk(
        configurationWithHashing(false), shim(SearchClientShim.SearchEngineType.OPENSEARCH_3));
    List<String> warnings = warnings();
    assertEquals(warnings.size(), 1);
    assertTrue(warnings.get(0).contains("512-byte"), warnings.get(0));
  }

  @Test
  public void warnsOnOpenSearch3WhenDocIdConfigurationIsAbsent() {
    ElasticSearchServiceFactory.warnOpenSearch3DocIdRisk(
        new ElasticSearchConfiguration(), shim(SearchClientShim.SearchEngineType.OPENSEARCH_3));
    assertEquals(warnings().size(), 1);
  }

  @Test
  public void staysQuietWhenHashingEnabledOrNotOpenSearch3OrNoClient() {
    ElasticSearchServiceFactory.warnOpenSearch3DocIdRisk(
        configurationWithHashing(true), shim(SearchClientShim.SearchEngineType.OPENSEARCH_3));
    ElasticSearchServiceFactory.warnOpenSearch3DocIdRisk(
        configurationWithHashing(false), shim(SearchClientShim.SearchEngineType.OPENSEARCH_2));
    ElasticSearchServiceFactory.warnOpenSearch3DocIdRisk(
        configurationWithHashing(false), shim(SearchClientShim.SearchEngineType.ELASTICSEARCH_8));
    ElasticSearchServiceFactory.warnOpenSearch3DocIdRisk(configurationWithHashing(false), null);
    assertTrue(warnings().isEmpty(), warnings().toString());
  }

  private List<String> warnings() {
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .collect(Collectors.toList());
  }

  private static SearchClientShim<?> shim(SearchClientShim.SearchEngineType engineType) {
    SearchClientShim<?> shim = mock(SearchClientShim.class);
    when(shim.getEngineType()).thenReturn(engineType);
    return shim;
  }

  private static ElasticSearchConfiguration configurationWithHashing(boolean hashIdEnabled) {
    EntityDocIdConfiguration schemaField = new EntityDocIdConfiguration();
    schemaField.setHashIdEnabled(hashIdEnabled);
    DocIdsConfiguration docIds = new DocIdsConfiguration();
    docIds.setSchemaField(schemaField);
    IndexConfiguration index = new IndexConfiguration();
    index.setDocIds(docIds);
    ElasticSearchConfiguration configuration = new ElasticSearchConfiguration();
    configuration.setIndex(index);
    return configuration;
  }
}
