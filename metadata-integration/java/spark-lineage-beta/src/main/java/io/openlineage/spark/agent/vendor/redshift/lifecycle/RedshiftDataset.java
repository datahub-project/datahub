package io.openlineage.spark.agent.vendor.redshift.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.SqlUtils;
import io.openlineage.spark.api.DatasetFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class RedshiftDataset {
  public static final String REDSHIFT_PREFIX = "redshift://";

  private static final Logger logger = LoggerFactory.getLogger(RedshiftDataset.class);
  public static final String DEFAULT_SCHEMA = "public";

  public static <D extends OpenLineage.Dataset> List<D> getDatasets(
      DatasetFactory<D> factory,
      String url,
      Optional<String> dbtable,
      Optional<String> query,
      StructType schema)
      throws URISyntaxException {

    URI jdbcUrl =
        new URI(
            REDSHIFT_PREFIX
                + url.replace("jdbc:redshift:iam://", "").replace("jdbc:redshift://", ""));
    String db = jdbcUrl.getPath().substring(1); // remove leading slash
    final String namespace =
        jdbcUrl.getScheme() + "://" + jdbcUrl.getHost() + ":" + jdbcUrl.getPort();

    final String tableName;
    // https://github.com/databricks/spark-redshift?tab=readme-ov-file
    // > Specify one of the following options for the table data to be read:
    // >    - `dbtable`: The name of the table to be read. All columns and records are retrieved
    // >      (i.e. it is equivalent to SELECT * FROM db_table).
    // >    - `query`: The exact query (SELECT statement) to run.
    // If dbtable is null it will be replaced with the string `complex` and it means the query
    // option was used.
    // An improvement could be put the query string in the `DatasetFacets`
    if (dbtable.isPresent()) {
      tableName = dbtable.get();
      String[] splits = tableName.split("\\.");
      String table = tableName;
      if (splits.length == 1) {
        table = String.format("%s.%s.%s", db, DEFAULT_SCHEMA, tableName);
      } else if (splits.length == 2) {
        table = String.format("%s.%s", db, tableName);
      } else if (splits.length == 3) {
        table = tableName;
      } else {
        logger.warn("Redshift getDataset: tableName: {} is not in the expected format", tableName);
        return Collections.emptyList();
      }

      return Collections.singletonList(factory.getDataset(table, namespace, schema));
    } else if (query.isPresent()) {
      return SqlUtils.getDatasets(factory, query.get(), "redshift", namespace, db, DEFAULT_SCHEMA);
    } else {
      logger.warn(
          "Unable to discover Redshift table property - neither \"dbtable\" nor \"query\" option present");
    }
    return Collections.emptyList();
  }
}
