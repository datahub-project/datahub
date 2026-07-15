package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.RoutingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.postgres.PostgresEntitySearchService;
import io.ebean.Database;
import org.springframework.beans.factory.ObjectProvider;
import org.testng.annotations.Test;

/**
 * {@link EntitySearchServiceFactory} wiring for {@code postgres.pgSearch.entity.query.enabled}
 * (PostgreSQL read path + OpenSearch writes), without starting full GMS.
 */
public class EntitySearchServiceFactoryTest {

  private static final String PG_JDBC = "jdbc:postgresql://localhost:5432/datahub";

  private static SearchServiceConfiguration testSearchService() {
    return SearchServiceConfiguration.builder()
        .limit(
            LimitConfig.builder()
                .results(ResultsLimitConfig.builder().apiDefault(100).max(1000).build())
                .build())
        .build();
  }

  private static ElasticSearchConfiguration elasticConfig(boolean enabled) {
    return ElasticSearchConfiguration.builder().enabled(enabled).build();
  }

  @SuppressWarnings("unchecked")
  private static ObjectProvider<Database> databaseProvider(Database db) {
    ObjectProvider<Database> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(db);
    return provider;
  }

  @SuppressWarnings("unchecked")
  private static ObjectProvider<Database> emptyDatabaseProvider() {
    ObjectProvider<Database> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(null);
    return provider;
  }

  @SuppressWarnings("unchecked")
  private static ObjectProvider<ElasticSearchService> elasticProvider(ElasticSearchService es) {
    ObjectProvider<ElasticSearchService> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(es);
    return provider;
  }

  @SuppressWarnings("unchecked")
  private static ObjectProvider<ElasticSearchService> emptyElasticProvider() {
    ObjectProvider<ElasticSearchService> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(null);
    return provider;
  }

  @Test
  public void queryDisabled_returnsElasticSearchServiceBean() throws Exception {
    EntitySearchServiceFactory factory = new EntitySearchServiceFactory();
    ElasticSearchService es = mock(ElasticSearchService.class);

    PostgresSqlSetupProperties props = PostgresSqlSetupProperties.disabled();
    ConfigurationProvider cp = mock(ConfigurationProvider.class);
    when(cp.getElasticSearch()).thenReturn(elasticConfig(true));
    when(cp.getSearchService()).thenReturn(testSearchService());

    ObjectProvider<Database> dbProvider = mock(ObjectProvider.class);

    EntitySearchService svc =
        factory.getInstance(
            emptyDatabaseProvider(), dbProvider, elasticProvider(es), props, cp, PG_JDBC);

    assertEquals(svc, es);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void queryEnabled_entityDisabled_throws() throws Exception {
    EntitySearchServiceFactory factory = new EntitySearchServiceFactory();

    PostgresSqlSetupProperties props = PostgresSqlSetupProperties.disabled();
    props.getPgSearch().getEntity().getQuery().setEnabled(true);

    ConfigurationProvider cp = mock(ConfigurationProvider.class);
    when(cp.getElasticSearch()).thenReturn(elasticConfig(true));
    when(cp.getSearchService()).thenReturn(testSearchService());

    ObjectProvider<Database> dbProvider = mock(ObjectProvider.class);
    when(dbProvider.getIfAvailable()).thenReturn(mock(Database.class));

    factory.getInstance(
        databaseProvider(mock(Database.class)),
        dbProvider,
        elasticProvider(mock(ElasticSearchService.class)),
        props,
        cp,
        PG_JDBC);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void queryEnabled_noDatabase_throws() throws Exception {
    EntitySearchServiceFactory factory = new EntitySearchServiceFactory();

    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.getPgSearch().getEntity().setEnabled(true);
    props.getPgSearch().getEntity().getQuery().setEnabled(true);
    props.getPgSearch().getEntity().setTablePrefix("md_search_test");

    ConfigurationProvider cp = mock(ConfigurationProvider.class);
    when(cp.getElasticSearch()).thenReturn(elasticConfig(true));
    when(cp.getSearchService()).thenReturn(testSearchService());

    ObjectProvider<Database> dbProvider = emptyDatabaseProvider();

    factory.getInstance(
        emptyDatabaseProvider(),
        dbProvider,
        elasticProvider(mock(ElasticSearchService.class)),
        props,
        cp,
        PG_JDBC);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void queryEnabled_blankEbeanUrl_throws() throws Exception {
    EntitySearchServiceFactory factory = new EntitySearchServiceFactory();

    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.getPgSearch().getEntity().setEnabled(true);
    props.getPgSearch().getEntity().getQuery().setEnabled(true);
    props.getPgSearch().getEntity().setTablePrefix("md_search_test");

    ConfigurationProvider cp = mock(ConfigurationProvider.class);
    when(cp.getElasticSearch()).thenReturn(elasticConfig(true));
    when(cp.getSearchService()).thenReturn(testSearchService());

    ObjectProvider<Database> dbProvider = mock(ObjectProvider.class);
    when(dbProvider.getIfAvailable()).thenReturn(mock(Database.class));

    factory.getInstance(
        emptyDatabaseProvider(),
        dbProvider,
        elasticProvider(mock(ElasticSearchService.class)),
        props,
        cp,
        "  ");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void queryEnabled_nonPostgresJdbc_throws() throws Exception {
    EntitySearchServiceFactory factory = new EntitySearchServiceFactory();

    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.getPgSearch().getEntity().setEnabled(true);
    props.getPgSearch().getEntity().getQuery().setEnabled(true);
    props.getPgSearch().getEntity().setTablePrefix("md_search_test");

    ConfigurationProvider cp = mock(ConfigurationProvider.class);
    when(cp.getElasticSearch()).thenReturn(elasticConfig(true));
    when(cp.getSearchService()).thenReturn(testSearchService());

    ObjectProvider<Database> dbProvider = mock(ObjectProvider.class);
    when(dbProvider.getIfAvailable()).thenReturn(mock(Database.class));

    factory.getInstance(
        emptyDatabaseProvider(),
        dbProvider,
        elasticProvider(mock(ElasticSearchService.class)),
        props,
        cp,
        "jdbc:mysql://localhost:3306/datahub");
  }

  @Test
  public void queryEnabled_postgres_returnsRoutingEntitySearchService() throws Exception {
    EntitySearchServiceFactory factory = new EntitySearchServiceFactory();

    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.getPgSearch().getEntity().setEnabled(true);
    props.getPgSearch().getEntity().getQuery().setEnabled(true);
    props.getPgSearch().getEntity().setTablePrefix("md_search_test");

    ConfigurationProvider cp = mock(ConfigurationProvider.class);
    when(cp.getElasticSearch()).thenReturn(elasticConfig(true));
    when(cp.getSearchService()).thenReturn(testSearchService());

    ObjectProvider<Database> dbProvider = mock(ObjectProvider.class);
    when(dbProvider.getIfAvailable()).thenReturn(mock(Database.class));

    EntitySearchService svc =
        factory.getInstance(
            emptyDatabaseProvider(),
            dbProvider,
            elasticProvider(mock(ElasticSearchService.class)),
            props,
            cp,
            PG_JDBC);

    assertTrue(svc instanceof RoutingEntitySearchService);
  }

  @Test
  public void elasticsearchDisabled_pgQueryEnabled_returnsPostgresEntitySearchService()
      throws Exception {
    EntitySearchServiceFactory factory = new EntitySearchServiceFactory();

    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.getPgSearch().getEntity().setEnabled(true);
    props.getPgSearch().getEntity().getQuery().setEnabled(true);
    props.getPgSearch().getEntity().setTablePrefix("md_search_test");

    ConfigurationProvider cp = mock(ConfigurationProvider.class);
    when(cp.getElasticSearch()).thenReturn(elasticConfig(false));
    when(cp.getSearchService()).thenReturn(testSearchService());

    ObjectProvider<Database> dbProvider = mock(ObjectProvider.class);
    when(dbProvider.getIfAvailable()).thenReturn(mock(Database.class));

    EntitySearchService svc =
        factory.getInstance(
            emptyDatabaseProvider(), dbProvider, emptyElasticProvider(), props, cp, PG_JDBC);

    assertTrue(svc instanceof PostgresEntitySearchService);
  }
}
