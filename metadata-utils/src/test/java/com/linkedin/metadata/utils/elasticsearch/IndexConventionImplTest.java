package com.linkedin.metadata.utils.elasticsearch;

import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.util.Pair;
import java.net.URLEncoder;
import java.util.Optional;
import org.testng.annotations.Test;

public class IndexConventionImplTest {

  @Test
  public void testIndexConventionNoPrefix() {
    IndexConvention indexConventionNoPrefix = IndexConventionImpl.noPrefix("MD5");
    String entityName = "dataset";
    String expectedIndexName = "datasetindex_v2";
    assertEquals(indexConventionNoPrefix.getEntityIndexName(entityName), expectedIndexName);
    assertEquals(indexConventionNoPrefix.getPrefix(), Optional.empty());
    assertEquals(indexConventionNoPrefix.getEntityName(expectedIndexName), Optional.of(entityName));
    assertEquals(indexConventionNoPrefix.getEntityName("totally not an index"), Optional.empty());
    assertEquals(indexConventionNoPrefix.getEntityName("dataset_v2"), Optional.empty());
    assertEquals(
        indexConventionNoPrefix.getEntityName("dashboardindex_v2_1683649932260"),
        Optional.of("dashboard"));
  }

  @Test
  public void testIndexConventionPrefix() {
    IndexConvention indexConventionPrefix =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix("prefix")
                .hashIdAlgo("MD5")
                .build());
    String entityName = "dataset";
    String expectedIndexName = "prefix_datasetindex_v2";
    assertEquals(indexConventionPrefix.getEntityIndexName(entityName), expectedIndexName);
    assertEquals(indexConventionPrefix.getPrefix(), Optional.of("prefix"));
    assertEquals(indexConventionPrefix.getEntityName(expectedIndexName), Optional.of(entityName));
    assertEquals(indexConventionPrefix.getEntityName("totally not an index"), Optional.empty());
    assertEquals(indexConventionPrefix.getEntityName("prefix_dataset_v2"), Optional.empty());
    assertEquals(
        indexConventionPrefix.getEntityName("prefix_dashboardindex_v2_1683649932260"),
        Optional.of("dashboard"));
    assertEquals(
        indexConventionPrefix.getEntityName("dashboardindex_v2_1683649932260"), Optional.empty());
  }

  @Test
  public void testTimeseriesIndexConventionNoPrefix() {
    IndexConvention indexConventionNoPrefix = IndexConventionImpl.noPrefix("MD5");
    String entityName = "dataset";
    String aspectName = "datasetusagestatistics";
    String expectedIndexName = "dataset_datasetusagestatisticsaspect_v1";
    assertEquals(
        indexConventionNoPrefix.getTimeseriesAspectIndexName(entityName, aspectName),
        expectedIndexName);
    assertEquals(indexConventionNoPrefix.getPrefix(), Optional.empty());
    assertEquals(
        indexConventionNoPrefix.getEntityAndAspectName(expectedIndexName),
        Optional.of(Pair.of(entityName, aspectName)));
    assertEquals(
        indexConventionNoPrefix.getEntityAndAspectName("totally not an index"), Optional.empty());
    assertEquals(indexConventionNoPrefix.getEntityAndAspectName("dataset_v2"), Optional.empty());
    assertEquals(
        indexConventionNoPrefix.getEntityAndAspectName(
            "dashboard_dashboardusagestatisticsaspect_v1"),
        Optional.of(Pair.of("dashboard", "dashboardusagestatistics")));
  }

  @Test
  public void testTimeseriesIndexConventionPrefix() {
    IndexConvention indexConventionPrefix =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix("prefix")
                .hashIdAlgo("MD5")
                .build());
    String entityName = "dataset";
    String aspectName = "datasetusagestatistics";
    String expectedIndexName = "prefix_dataset_datasetusagestatisticsaspect_v1";
    assertEquals(
        indexConventionPrefix.getTimeseriesAspectIndexName(entityName, aspectName),
        expectedIndexName);
    assertEquals(indexConventionPrefix.getPrefix(), Optional.of("prefix"));
    assertEquals(
        indexConventionPrefix.getEntityAndAspectName(expectedIndexName),
        Optional.of(Pair.of(entityName, aspectName)));
    assertEquals(
        indexConventionPrefix.getEntityAndAspectName("totally not an index"), Optional.empty());
    assertEquals(
        indexConventionPrefix.getEntityAndAspectName("prefix_datasetusagestatisticsaspect_v1"),
        Optional.empty());
  }

  @Test
  public void testSchemaFieldDocumentId() {
    assertEquals(
        new IndexConventionImpl(
                IndexConventionImpl.IndexConventionConfig.builder()
                    .prefix("")
                    .hashIdAlgo("")
                    .schemaFieldDocIdHashEnabled(true)
                    .build())
            .getEntityDocumentId(
                UrnUtils.getUrn(
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.factor_income,PROD),year)")),
        URLEncoder.encode(
            String.format(
                "urn:li:schemaField:(%s,%s)",
                sha256Hex(
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.factor_income,PROD)"),
                sha256Hex("year"))));
  }
}
