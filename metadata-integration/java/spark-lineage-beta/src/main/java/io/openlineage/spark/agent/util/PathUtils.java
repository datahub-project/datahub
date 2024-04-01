/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import datahub.spark.conf.SparkAppContext;
import datahub.spark.conf.SparkConfigParser;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.HdfsPathDataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifierUtils;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.internal.StaticSQLConf;

@Slf4j
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class PathUtils {

  private static final String DEFAULT_SCHEME = "file";
  public static final String SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN =
      "spark.openlineage.dataset.removePath.pattern";
  public static final String REMOVE_PATTERN_GROUP = "remove";

  private static Optional<SparkConf> sparkConf = Optional.empty();

  public static DatasetIdentifier fromPath(Path path) {
    return fromPath(path, DEFAULT_SCHEME);
  }

  public static DatasetIdentifier fromPath(Path path, String defaultScheme) {
    return fromURI(path.toUri(), defaultScheme);
  }

  public static DatasetIdentifier fromURI(URI location) {
    return fromURI(location, DEFAULT_SCHEME);
  }

  public static DatasetIdentifier fromURI(URI location, String defaultScheme) {
    DatasetIdentifier di = DatasetIdentifierUtils.fromURI(location, defaultScheme);
    return new DatasetIdentifier(removePathPattern(di.getName()), di.getNamespace());
  }

  public static DatasetIdentifier fromCatalogTable(CatalogTable catalogTable) {
    return fromCatalogTable(catalogTable, loadSparkConf());
  }

  /**
   * Create DatasetIdentifier from CatalogTable, using storage's locationURI if it exists. In other
   * way, use defaultTablePath.
   */
  @SneakyThrows
  public static DatasetIdentifier fromCatalogTable(
      CatalogTable catalogTable, Optional<SparkConf> sparkConf) {

    DatasetIdentifier di;
    if (catalogTable.storage() != null && catalogTable.storage().locationUri().isDefined()) {
      di = PathUtils.fromURI(catalogTable.storage().locationUri().get(), DEFAULT_SCHEME);
    } else {
      // try to obtain location
      try {
        di = prepareDatasetIdentifierFromDefaultTablePath(catalogTable);
      } catch (IllegalStateException e) {
        // session inactive - no way to find DatasetProvider
        throw new IllegalArgumentException(
            "Unable to extract DatasetIdentifier from a CatalogTable", e);
      }
    }

    Optional<URI> metastoreUri = extractMetastoreUri(sparkConf);
    // TODO: Is the call to "metastoreUri.get()" really needed?
    //   Java's Optional should prevent the null in the first place.
    if (metastoreUri.isPresent() && metastoreUri.get() != null) {
      // dealing with Hive tables
      DatasetIdentifier symlink = prepareHiveDatasetIdentifier(catalogTable, metastoreUri.get());
      return di.withSymlink(
          symlink.getName(), symlink.getNamespace(), DatasetIdentifier.SymlinkType.TABLE);
    } else {
      return di.withSymlink(
          nameFromTableIdentifier(catalogTable.identifier()),
          StringUtils.substringBeforeLast(di.getName(), File.separator),
          DatasetIdentifier.SymlinkType.TABLE);
    }
  }

  @SneakyThrows
  private static DatasetIdentifier prepareDatasetIdentifierFromDefaultTablePath(
      CatalogTable catalogTable) {
    URI uri =
        SparkSession.active().sessionState().catalog().defaultTablePath(catalogTable.identifier());

    return PathUtils.fromURI(uri);
  }

  @SneakyThrows
  private static DatasetIdentifier prepareHiveDatasetIdentifier(
      CatalogTable catalogTable, URI metastoreUri) {
    String qualifiedName = nameFromTableIdentifier(catalogTable.identifier());
    if (!qualifiedName.startsWith("/")) {
      qualifiedName = String.format("/%s", qualifiedName);
    }
    return PathUtils.fromPath(
        new Path(enrichHiveMetastoreURIWithTableName(metastoreUri, qualifiedName)));
  }

  @SneakyThrows
  public static URI enrichHiveMetastoreURIWithTableName(URI metastoreUri, String qualifiedName) {
    return new URI(
        "hive", null, metastoreUri.getHost(), metastoreUri.getPort(), qualifiedName, null, null);
  }

  /**
   * SparkConf does not change through job lifetime but it can get lost once session is closed. It's
   * good to have it set in case of SPARK-29046
   */
  private static Optional<SparkConf> loadSparkConf() {
    if (!sparkConf.isPresent() && SparkSession.getDefaultSession().isDefined()) {
      sparkConf = Optional.of(SparkSession.getDefaultSession().get().sparkContext().getConf());
    }
    return sparkConf;
  }

  private static Optional<URI> extractMetastoreUri(Optional<SparkConf> sparkConf) {
    // make sure SparkConf is present
    if (!sparkConf.isPresent()) {
      return Optional.empty();
    }

    // make sure enableHiveSupport is called
    Optional<String> setting =
        SparkConfUtils.findSparkConfigKey(
            sparkConf.get(), StaticSQLConf.CATALOG_IMPLEMENTATION().key());
    if (!setting.isPresent() || !"hive".equals(setting.get())) {
      return Optional.empty();
    }

    return SparkConfUtils.getMetastoreUri(sparkConf.get());
  }

  private static String removeFirstSlashIfSingleSlashInString(String name) {
    if (name.chars().filter(x -> x == '/').count() == 1 && name.startsWith("/")) {
      return name.substring(1);
    }
    return name;
  }

  private static String removePathPattern(String datasetName) {
    // TODO: The reliance on global-mutable state here should be changed
    //  this led to problems in the PathUtilsTest class, where some tests interfered with others
    log.info("Removing path pattern from dataset name {}", datasetName);
    Optional<SparkConf> conf = loadSparkConf();
    if (!conf.isPresent()) {
      return datasetName;
    }
    try {
      String propertiesString =
          Arrays.stream(conf.get().getAllWithPrefix("spark.datahub."))
              .map(tup -> tup._1 + "= \"" + tup._2 + "\"")
              .collect(Collectors.joining("\n"));
      Config datahubConfig = ConfigFactory.parseString(propertiesString);
      DatahubOpenlineageConfig datahubOpenlineageConfig =
          SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
              datahubConfig, new SparkAppContext());
      HdfsPathDataset hdfsPath =
          HdfsPathDataset.create(new URI(datasetName), datahubOpenlineageConfig);
      log.debug("Transformed path is {}", hdfsPath.getDatasetPath());
      return hdfsPath.getDatasetPath();
    } catch (InstantiationException e) {
      log.warn(
          "Unable to convert dataset {} to path the exception was {}", datasetName, e.getMessage());
      return datasetName;
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static String nameFromTableIdentifier(TableIdentifier identifier) {
    // we create name instead of calling `unquotedString` method which includes spark_catalog
    // for Spark 3.4
    String name;
    if (identifier.database().isDefined()) {
      // include database in name
      name = String.format("%s.%s", identifier.database().get(), identifier.table());
    } else {
      // just table name
      name = identifier.table();
    }

    return name;
  }
}
