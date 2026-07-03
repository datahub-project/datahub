/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;

/**
 * DataHub customization of OpenLineage's {@code AwsUtils}: backports upstream PR #4384 ("Fix spark
 * iceberg glue catalog symlink") onto the bundled OpenLineage 1.38.0 class.
 *
 * <p>OpenLineage 1.38 (PR #4053, "fix false hive glue detection") narrowed {@code getGlueArn} to
 * the Hive-on-Glue metastore case only. That silently dropped the {@code
 * arn:aws:glue:<region>:<account>} symlink for Iceberg tables read through {@code GlueCatalog}, and
 * downstream {@code GlueCatalogTypeHandler.getIdentifier} wraps the empty result in {@code
 * Optional.of(...)} — an NPE that the plan visitor swallows, so the Iceberg input dataset is never
 * emitted at all. DataHub relies on that symlink to resolve the per-connection {@code
 * platform_instance} for cross-platform Spark lineage.
 *
 * <p>The fix mirrors OL #4384 rather than the pre-1.38 blanket fallback (which would reintroduce
 * the false-positive detection #4053 removed by stamping a Glue ARN on every non-Hive table):
 * detect Iceberg-on-Glue explicitly from the {@code spark.sql.catalog.*} catalog-impl and build the
 * ARN only then. Remove this file once the agent upgrades to an OpenLineage release that already
 * contains #4384 (post-1.38).
 */
@Slf4j
public final class AwsUtils {
  public static final String HIVE_METASTORE_CLIENT_FACTORY_CLASS =
      "hive.metastore.client.factory.class";
  public static final String AWS_GLUE_HIVE_FACTORY_CLASS =
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory";
  private static final String HIVE_METASTORE_GLUE_CATALOG_ID_KEY = "hive.metastore.glue.catalogid";
  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";
  private static final String GLUE_CATALOG_SUFFIX = "GlueCatalog";

  public static Optional<String> getGlueArn(SparkConf sparkConf, Configuration hadoopConf) {
    if (isHiveUsingGlue(sparkConf, hadoopConf) || isIcebergUsingGlue(sparkConf)) {
      return awsRegion()
          .flatMap(
              region ->
                  getGlueCatalogId(sparkConf, hadoopConf)
                      .map(glueCatalogId -> "arn:aws:glue:" + region + ":" + glueCatalogId));
    }
    return Optional.empty();
  }

  private static Optional<String> getGlueCatalogId(SparkConf sparkConf, Configuration hadoopConf) {
    Optional<String> explicitGlueCatalogId = getExplicitGlueCatalogId(sparkConf, hadoopConf);
    if (explicitGlueCatalogId.isPresent()) {
      return explicitGlueCatalogId;
    }
    Optional<String> glueJobAccountId =
        SparkConfUtils.findSparkConfigKey(sparkConf, "spark.glue.accountId");
    if (glueJobAccountId.isPresent()) {
      log.debug(
          "Using [spark.glue.accountId] property [{}] as catalog ID.", glueJobAccountId.get());
      return glueJobAccountId;
    }
    log.debug("Fetching current account ID to use as the catalog ID.");
    return Optional.ofNullable(AwsAccountIdFetcher.getAccountId());
  }

  private static Optional<String> getExplicitGlueCatalogId(
      SparkConf sparkConf, Configuration hadoopConf) {
    Optional<String> sparkPropertyCatalogId =
        SparkConfUtils.findSparkConfigKey(sparkConf, HIVE_METASTORE_GLUE_CATALOG_ID_KEY);
    if (sparkPropertyCatalogId.isPresent()) {
      log.debug(
          "There is an explicit catalog ID [{}] passed as [{}] Spark property.",
          sparkPropertyCatalogId.get(),
          HIVE_METASTORE_GLUE_CATALOG_ID_KEY);
      return sparkPropertyCatalogId;
    }
    Optional<String> hadoopPropertyCatalogId =
        SparkConfUtils.findHadoopConfigKey(hadoopConf, HIVE_METASTORE_GLUE_CATALOG_ID_KEY);
    hadoopPropertyCatalogId.ifPresent(
        s ->
            log.debug(
                "There is an explicit catalog ID [{}] passed as [{}] Hadoop property.",
                s,
                HIVE_METASTORE_GLUE_CATALOG_ID_KEY));
    return hadoopPropertyCatalogId;
  }

  private static Optional<String> awsRegion() {
    return Optional.ofNullable(System.getenv("AWS_DEFAULT_REGION"))
        .filter(s -> !s.isEmpty())
        .map(Optional::of)
        .orElseGet(() -> Optional.ofNullable(System.getenv("AWS_REGION")));
  }

  private static boolean isIcebergUsingGlue(SparkConf sparkConf) {
    return Arrays.stream(sparkConf.getAllWithPrefix(SPARK_SQL_CATALOG_PREFIX))
        .anyMatch(tuple -> tuple._2().endsWith(GLUE_CATALOG_SUFFIX));
  }

  private static boolean isHiveUsingGlue(SparkConf sparkConf, Configuration hadoopConf) {
    Optional<String> hadoopFactoryClass =
        SparkConfUtils.findHadoopConfigKey(hadoopConf, HIVE_METASTORE_CLIENT_FACTORY_CLASS);
    Optional<String> sparkFactoryClass =
        SparkConfUtils.findSparkConfigKey(sparkConf, HIVE_METASTORE_CLIENT_FACTORY_CLASS);
    return AWS_GLUE_HIVE_FACTORY_CLASS.equals(
        hadoopFactoryClass.orElse(sparkFactoryClass.orElse(null)));
  }

  private AwsUtils() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }
}
