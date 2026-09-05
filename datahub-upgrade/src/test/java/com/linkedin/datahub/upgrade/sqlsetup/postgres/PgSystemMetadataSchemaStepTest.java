package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgSystemMetadataSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.testng.annotations.Test;

public class PgSystemMetadataSchemaStepTest {

  @Test
  public void nullOptions_returnsFailed() {
    PostgresSqlSetupProperties props = mock(PostgresSqlSetupProperties.class);
    when(props.buildPgSystemMetadataOptions()).thenReturn(null);
    PgSystemMetadataSchemaStep step = new PgSystemMetadataSchemaStep(mock(Database.class), props);

    UpgradeContext context = mock(UpgradeContext.class);
    when(context.report()).thenReturn(mock(UpgradeReport.class));

    UpgradeStepResult result = step.executable().apply(context);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void connectionFailure_returnsFailed() throws Exception {
    PostgresSqlSetupProperties props = mock(PostgresSqlSetupProperties.class);
    PgSystemMetadataSetupOptions options = mock(PgSystemMetadataSetupOptions.class);
    when(options.getPoolUrl()).thenReturn("");
    when(props.buildPgSystemMetadataOptions()).thenReturn(options);

    Database database = mock(Database.class);
    DataSource dataSource = mock(DataSource.class);
    when(database.dataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenThrow(new SQLException("unavailable"));

    PgSystemMetadataSchemaStep step = new PgSystemMetadataSchemaStep(database, props);
    UpgradeContext context = mock(UpgradeContext.class);
    when(context.report()).thenReturn(mock(UpgradeReport.class));

    UpgradeStepResult result = step.executable().apply(context);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }
}
