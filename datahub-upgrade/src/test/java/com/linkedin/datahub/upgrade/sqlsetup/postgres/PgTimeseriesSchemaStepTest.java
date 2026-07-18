package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.PostgresPartmanSqlSetupSupport;
import io.ebean.Database;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgTimeseriesSchemaStepTest {

  private Database mockDatabase;
  private PgTimeseriesSchemaStep step;

  @BeforeMethod
  public void setUp() {
    mockDatabase = Mockito.mock(Database.class);
    PostgresSqlSetupProperties props = PostgresSqlSetupProperties.disabled();
    props.getPgTimeseries().setEnabled(true);
    step = new PgTimeseriesSchemaStep(mockDatabase, props);
  }

  @Test
  public void testId() {
    assertEquals(step.id(), "PgTimeseriesSchemaStep");
  }

  @Test
  public void testTimeseriesPartmanRetentionUpdateSqlEmptyWhenNull() {
    assertEquals(
        PostgresPartmanSqlSetupSupport.partmanRetentionUpdateSql(
            "partman", "datahub", null, "pgtimeseries_aspect_row"),
        "");
    assertEquals(
        PostgresPartmanSqlSetupSupport.partmanRetentionUpdateSql(
            "partman", "datahub", "", "pgtimeseries_aspect_row"),
        "");
  }

  @Test
  public void testTimeseriesPartmanRetentionUpdateSqlNonEmpty() {
    String sql =
        PostgresPartmanSqlSetupSupport.partmanRetentionUpdateSql(
            "partman", "datahub", "9 days", "pgtimeseries_aspect_row");
    assertTrue(sql.contains("UPDATE \"partman\".part_config"));
    assertTrue(sql.contains("datahub.pgtimeseries_aspect_row"));
    assertTrue(sql.contains("'9 days'"));
  }
}
