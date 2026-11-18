package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.linkedin.common.urn.DatasetUrn;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class JdbcDatasetNamingTest {
  private static final String POSTGRES_URL = "jdbc:postgresql://host:5432/app";
  private static final String SQLSERVER_URL = "jdbc:sqlserver://host:1433;databaseName=app";

  @Test
  void fullyQualifiedPostgresTableDoesNotRepeatDatabase() {
    assertDatasetUrn(POSTGRES_URL, "app.public.orders", "postgres", "app.public.orders");
  }

  @Test
  void crossDatabaseSqlServerTableUsesItsOwnDatabase() {
    assertDatasetUrn(SQLSERVER_URL, "otherdb.dbo.orders", "mssql", "otherdb.dbo.orders");
  }

  @Test
  void twoPartPostgresTableUsesConnectionDatabase() {
    assertDatasetUrn(POSTGRES_URL, "public.orders", "postgres", "app.public.orders");
  }

  @Test
  void twoPartSqlServerTableUsesConnectionDatabase() {
    assertDatasetUrn(SQLSERVER_URL, "dbo.orders", "mssql", "app.dbo.orders");
  }

  private static void assertDatasetUrn(
      String url, String dbtable, String platform, String expectedName) {
    DatasetIdentifier identifier =
        JdbcDatasetUtils.getDatasetIdentifier(url, dbtable, new Properties());
    OpenLineage.InputDataset dataset =
        new OpenLineage.InputDatasetBuilder()
            .namespace(identifier.getNamespace())
            .name(identifier.getName())
            .build();
    DatasetUrn urn =
        OpenLineageToDataHub.convertOpenlineageDatasetToDatasetUrn(
                dataset, DatahubOpenlineageConfig.builder().build())
            .orElseThrow();

    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:" + platform + "," + expectedName + ",PROD)",
        urn.toString());
  }
}
