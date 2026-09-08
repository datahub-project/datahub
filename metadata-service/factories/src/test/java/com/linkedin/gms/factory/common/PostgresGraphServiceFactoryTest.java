package com.linkedin.gms.factory.common;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.Database;
import org.springframework.beans.factory.ObjectProvider;
import org.testng.annotations.Test;

public class PostgresGraphServiceFactoryTest {

  private static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/datahub";
  private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/datahub";

  @Test
  public void rejectsPostgresTypeWhenPgGraphDisabled() {
    PostgresGraphServiceFactory factory = new PostgresGraphServiceFactory();
    PostgresSqlSetupProperties props = PostgresSqlSetupProperties.disabled();
    expectThrows(
        IllegalStateException.class,
        () ->
            factory.postgresGraphService(
                mock(EntityRegistry.class),
                mock(ConfigurationProvider.class),
                mock(ObjectProvider.class),
                props,
                POSTGRES_URL));
  }

  @Test
  public void rejectsMissingEbeanBean() {
    PostgresGraphServiceFactory factory = new PostgresGraphServiceFactory();
    ObjectProvider<Database> databases = mock(ObjectProvider.class);
    when(databases.getIfAvailable()).thenReturn(null);
    expectThrows(
        IllegalStateException.class,
        () ->
            factory.postgresGraphService(
                mock(EntityRegistry.class),
                mock(ConfigurationProvider.class),
                databases,
                enabledProps(),
                POSTGRES_URL));
  }

  @Test
  public void rejectsBlankPoolUrl() {
    PostgresGraphServiceFactory factory = new PostgresGraphServiceFactory();
    ObjectProvider<Database> databases = mock(ObjectProvider.class);
    when(databases.getIfAvailable()).thenReturn(mock(Database.class));
    expectThrows(
        IllegalStateException.class,
        () ->
            factory.postgresGraphService(
                mock(EntityRegistry.class),
                mock(ConfigurationProvider.class),
                databases,
                enabledProps(),
                "  "));
  }

  @Test
  public void rejectsNonPostgresJdbcUrl() {
    PostgresGraphServiceFactory factory = new PostgresGraphServiceFactory();
    ObjectProvider<Database> databases = mock(ObjectProvider.class);
    when(databases.getIfAvailable()).thenReturn(mock(Database.class));
    expectThrows(
        IllegalStateException.class,
        () ->
            factory.postgresGraphService(
                mock(EntityRegistry.class),
                mock(ConfigurationProvider.class),
                databases,
                enabledProps(),
                MYSQL_URL));
  }

  private static PostgresSqlSetupProperties enabledProps() {
    PostgresSqlSetupProperties props = PostgresSqlSetupProperties.disabled();
    props.setSchema("public");
    props.getPgGraph().setEnabled(true);
    props.getPgGraph().setTablePrefix("metadata_graph");
    props.getPgGraph().setPartitionCount(2);
    props.getPgGraph().setIdHashAlgo("XXHASH64");
    props.getPgGraph().setMaxEdgeWriteBatchSize(1000);
    return props;
  }
}
