package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgGraphSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.testng.annotations.Test;

public class PgGraphSchemaStepTest {

  @Test
  public void nullOptions_returnsFailed() {
    PostgresSqlSetupProperties props = mock(PostgresSqlSetupProperties.class);
    when(props.buildPgGraphOptions()).thenReturn(null);
    PgGraphSchemaStep step = new PgGraphSchemaStep(mock(Database.class), props);

    UpgradeContext context = mock(UpgradeContext.class);
    when(context.report()).thenReturn(mock(UpgradeReport.class));

    UpgradeStepResult result = step.executable().apply(context);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void connectionFailure_returnsFailed() throws Exception {
    PostgresSqlSetupProperties props = mock(PostgresSqlSetupProperties.class);
    PgGraphSetupOptions options = mock(PgGraphSetupOptions.class);
    when(options.getPoolUrl()).thenReturn("");
    when(props.buildPgGraphOptions()).thenReturn(options);

    Database database = mock(Database.class);
    DataSource dataSource = mock(DataSource.class);
    when(database.dataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenThrow(new SQLException("unavailable"));

    PgGraphSchemaStep step = new PgGraphSchemaStep(database, props);
    UpgradeContext context = mock(UpgradeContext.class);
    when(context.report()).thenReturn(mock(UpgradeReport.class));

    UpgradeStepResult result = step.executable().apply(context);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }
}
