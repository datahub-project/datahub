/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.internal.StaticSQLConf;

@Slf4j
@SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
public class PathUtils {
  private static final String DEFAULT_DB = "default";
  public static final String GLUE_TABLE_PREFIX = "table/";

  public static DatasetIdentifier fromPath(Path path) {
    return fromURI(path.toUri());
  }

  public static DatasetIdentifier fromURI(URI location) {
    return FilesystemDatasetUtils.fromLocation(location);
  }

  /**
   * Create DatasetIdentifier from CatalogTable, using storage's locationURI if it exists. In other
   * way, use defaultTablePath.
   */
  public static DatasetIdentifier fromCatalogTable(
      CatalogTable catalogTable, SparkSession sparkSession) {
    URI locationUri;
    locationUri = getLocationUri(catalogTable, sparkSession);
    return fromCatalogTable(catalogTable, sparkSession, locationUri);
  }

  /** Create DatasetIdentifier from CatalogTable, using provided location. */
  @SneakyThrows
  public static DatasetIdentifier fromCatalogTable(
      CatalogTable catalogTable, SparkSession sparkSession, Path location) {
    return fromCatalogTable(catalogTable, sparkSession, location.toUri());
  }

  /** Create DatasetIdentifier from CatalogTable, using provided location. */
  @SneakyThrows
  public static DatasetIdentifier fromCatalogTable(
      CatalogTable catalogTable, SparkSession sparkSession, URI location) {
    // perform URL normalization
    DatasetIdentifier locationDataset = fromURI(location);
    URI locationUri = FilesystemDatasetUtils.toLocation(locationDataset);

    Optional<DatasetIdentifier> symlinkDataset = Optional.empty();

    SparkContext sparkContext = sparkSession.sparkContext();
    SparkConf sparkConf = sparkContext.getConf();
    Configuration hadoopConf = sparkContext.hadoopConfiguration();

    Optional<URI> metastoreUri = getMetastoreUri(sparkContext);
    Optional<String> glueArn = AwsUtils.getGlueArn(sparkConf, hadoopConf);

    if (glueArn.isPresent()) {
      // Even if glue catalog is used, it will have a hive metastore URI
      // Use ARN format 'arn:aws:glue:{region}:{account_id}:table/{database}/{table}'
      String tableName = nameFromTableIdentifier(catalogTable.identifier(), "/");
      symlinkDataset =
          Optional.of(new DatasetIdentifier(GLUE_TABLE_PREFIX + tableName, glueArn.get()));
    } else if (metastoreUri.isPresent()) {
      // dealing with Hive tables
      URI hiveUri = prepareHiveUri(metastoreUri.get());
      String tableName = nameFromTableIdentifier(catalogTable.identifier());
      symlinkDataset = Optional.of(FilesystemDatasetUtils.fromLocationAndName(hiveUri, tableName));
    } else {
      Optional<URI> warehouseLocation =
          getWarehouseLocation(sparkConf, hadoopConf)
              // perform normalization
              .map(FilesystemDatasetUtils::fromLocation)
              .map(FilesystemDatasetUtils::toLocation);

      if (warehouseLocation.isPresent()) {
        URI relativePath = warehouseLocation.get().relativize(locationUri);
        if (!relativePath.equals(locationUri)) {
          // if there is no metastore, and table has custom location,
          // it cannot be accessed via default warehouse location
          String tableName = nameFromTableIdentifier(catalogTable.identifier());
          symlinkDataset =
              Optional.of(
                  FilesystemDatasetUtils.fromLocationAndName(warehouseLocation.get(), tableName));
        } else {
          // Table is outside warehouse, but we create symlink to actual location + tableName
          String tableName = nameFromTableIdentifier(catalogTable.identifier());
          symlinkDataset =
              Optional.of(FilesystemDatasetUtils.fromLocationAndName(locationUri, tableName));
        }
      }
    }

    if (symlinkDataset.isPresent()) {
      locationDataset.withSymlink(
          symlinkDataset.get().getName(),
          symlinkDataset.get().getNamespace(),
          DatasetIdentifier.SymlinkType.TABLE);
    }

    return locationDataset;
  }

  public static URI getDefaultLocationUri(SparkSession sparkSession, TableIdentifier identifier) {
    return sparkSession.sessionState().catalog().defaultTablePath(identifier);
  }

  public static Path reconstructDefaultLocation(String warehouse, String[] namespace, String name) {
    String database = null;
    if (namespace.length == 1) {
      // {"database"}
      database = namespace[0];
    } else if (namespace.length > 1) {
      // {"spark_catalog", "database"}
      database = namespace[1];
    }

    // /warehouse/mytable
    if (database == null || database.equals(DEFAULT_DB)) {
      return new Path(warehouse, name);
    }

    // /warehouse/mydb.db/mytable
    return new Path(warehouse, database + ".db", name);
  }

  public static Optional<URI> getMetastoreUri(SparkContext context) {
    // make sure enableHiveSupport is called
    Optional<String> setting =
        SparkConfUtils.findSparkConfigKey(
            context.getConf(), StaticSQLConf.CATALOG_IMPLEMENTATION().key());
    if (!setting.isPresent() || !"hive".equals(setting.get())) {
      return Optional.empty();
    }
    return SparkConfUtils.getMetastoreUri(context);
  }

  @SneakyThrows
  public static URI prepareHiveUri(URI uri) {
    return new URI("hive", uri.getAuthority(), null, null, null);
  }

  @SneakyThrows
  public static Optional<URI> getWarehouseLocation(SparkConf sparkConf, Configuration hadoopConf) {
    Optional<String> warehouseLocation =
        SparkConfUtils.findSparkConfigKey(sparkConf, StaticSQLConf.WAREHOUSE_PATH().key());
    if (!warehouseLocation.isPresent()) {
      warehouseLocation =
          SparkConfUtils.findHadoopConfigKey(hadoopConf, "hive.metastore.warehouse.dir");
    }
    return warehouseLocation.map(URI::create);
  }

  private static URI getLocationUri(CatalogTable catalogTable, SparkSession sparkSession) {
    URI locationUri;
    if (catalogTable.storage() != null && catalogTable.storage().locationUri().isDefined()) {
      locationUri = catalogTable.storage().locationUri().get();
    } else {
      locationUri = getDefaultLocationUri(sparkSession, catalogTable.identifier());
    }
    return locationUri;
  }

  /** Get DatasetIdentifier name in format database.table or table */
  private static String nameFromTableIdentifier(TableIdentifier identifier) {
    return nameFromTableIdentifier(identifier, ".");
  }

  private static String nameFromTableIdentifier(TableIdentifier identifier, String delimiter) {
    // calling `unquotedString` method includes `spark_catalog`, so instead get proper identifier
    // manually
    String name;
    if (identifier.database().isDefined()) {
      // include database in name
      name = identifier.database().get() + delimiter + identifier.table();
    } else {
      // just table name
      name = identifier.table();
    }

    return name;
  }
}
