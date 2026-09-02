/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.agent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.internal.SessionState;
import org.junit.jupiter.api.Test;
import scala.Option;

class PathUtilsTest {
  @Test
  void unityCatalogSymlinkUsesStableQualifiedIdentity() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.databricks.workspaceUrl", "https://workspace.example.com")
            .set("spark.databricks.unityCatalog.enabled", "true");
    SparkContext sparkContext = mock(SparkContext.class);
    Configuration hadoopConf = mock(Configuration.class);
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);
    TableIdentifier identifier =
        new TableIdentifier("orders", Option.apply("sales"), Option.apply("main"));

    DatasetIdentifier dataset =
        PathUtils.fromTableIdentifier(
            identifier,
            sparkContext,
            URI.create("s3://bucket/managed/tables/12345678-1234-1234-1234-123456789abc"));

    assertEquals("managed/tables/12345678-1234-1234-1234-123456789abc", dataset.getName());
    assertEquals("s3://bucket", dataset.getNamespace());
    assertEquals(1, dataset.getSymlinks().size());
    DatasetIdentifier.Symlink symlink = dataset.getSymlinks().get(0);
    assertEquals("main.sales.orders", symlink.getName());
    assertEquals(DatabricksUtils.UNITY_CATALOG_SYMLINK_NAMESPACE, symlink.getNamespace());
    assertSame(DatasetIdentifier.SymlinkType.TABLE, symlink.getType());
  }

  @Test
  void getLocationUriUsesExplicitLocationOrCatalogDefault() {
    CatalogTable catalogTable = mock(CatalogTable.class);
    CatalogStorageFormat storage = mock(CatalogStorageFormat.class);
    SparkSession sparkSession = mock(SparkSession.class);
    URI explicitLocation = URI.create("s3://bucket/explicit/orders");
    when(catalogTable.storage()).thenReturn(storage);
    when(storage.locationUri()).thenReturn(Option.apply(explicitLocation));

    assertEquals(explicitLocation, PathUtils.getLocationUri(catalogTable, sparkSession));

    TableIdentifier identifier = new TableIdentifier("orders", Option.apply("sales"));
    SessionState sessionState = mock(SessionState.class);
    SessionCatalog sessionCatalog = mock(SessionCatalog.class);
    URI defaultLocation = URI.create("file:/warehouse/sales.db/orders");
    when(storage.locationUri()).thenReturn(Option.empty());
    when(catalogTable.identifier()).thenReturn(identifier);
    when(sparkSession.sessionState()).thenReturn(sessionState);
    when(sessionState.catalog()).thenReturn(sessionCatalog);
    when(sessionCatalog.defaultTablePath(identifier)).thenReturn(defaultLocation);

    assertEquals(defaultLocation, PathUtils.getLocationUri(catalogTable, sparkSession));
  }

  @Test
  void tableOutsideWarehouseKeepsLocationBasedSymlink() {
    SparkConf sparkConf =
        new SparkConf().set("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse");
    SparkContext sparkContext = mock(SparkContext.class);
    Configuration hadoopConf = mock(Configuration.class);
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);
    TableIdentifier identifier = new TableIdentifier("orders", Option.apply("sales"));
    URI customLocation = URI.create("hdfs://namenode:8020/custom/orders");

    DatasetIdentifier dataset =
        PathUtils.fromTableIdentifier(identifier, sparkContext, customLocation);

    assertEquals(1, dataset.getSymlinks().size());
    DatasetIdentifier.Symlink symlink = dataset.getSymlinks().get(0);
    assertEquals("sales.orders", symlink.getName());
    assertEquals("hdfs://namenode:8020/custom/orders", symlink.getNamespace());
    assertSame(DatasetIdentifier.SymlinkType.TABLE, symlink.getType());
  }
}
