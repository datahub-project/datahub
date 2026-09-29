package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties.PgCron;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.testng.annotations.Test;

public class PgCronAdminConnectionsTest {

  @Test(expectedExceptions = IllegalStateException.class)
  public void openFailsWhenJdbcUrlMissing() throws SQLException {
    PgCronAdminConnections.open(new PostgresSqlSetupProperties());
  }

  @Test
  public void openUsesPasswordCredentials() throws SQLException {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    PgCron pgCron = new PgCron();
    PgCron.Admin admin = new PgCron.Admin();
    admin.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
    admin.setUsername("cron");
    admin.setPassword("secret");
    pgCron.setAdmin(admin);
    props.setPgCron(pgCron);

    try (var driverManager = mockStatic(DriverManager.class)) {
      Connection connection = mock(Connection.class);
      driverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenReturn(connection);

      assertNotNull(PgCronAdminConnections.open(props));
    }
  }
}
