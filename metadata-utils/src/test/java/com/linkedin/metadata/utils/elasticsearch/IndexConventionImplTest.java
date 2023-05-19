package com.linkedin.metadata.utils.elasticsearch;

import java.util.Optional;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class IndexConventionImplTest{

  @Test
  public void testIndexConventionNoPrefix() {
    IndexConvention indexConventionNoPrefix = new IndexConventionImpl(null);
    String entityName = "dataset";
    String expectedIndexName = "datasetindex_v2";
    assertEquals(indexConventionNoPrefix.getEntityIndexName(entityName), expectedIndexName);
    assertEquals(indexConventionNoPrefix.getPrefix(), Optional.empty());
    assertEquals(indexConventionNoPrefix.getEntityName(expectedIndexName), Optional.of(entityName));
    assertEquals(indexConventionNoPrefix.getEntityName("totally not an index"), Optional.empty());
    assertEquals(indexConventionNoPrefix.getEntityName("dataset_v2"), Optional.empty());
    assertEquals(indexConventionNoPrefix.getEntityName("dashboardindex_v2_1683649932260"), Optional.of("dashboard"));
  }

  @Test
  public void testIndexConventionPrefix() {
    IndexConvention indexConventionPrefix = new IndexConventionImpl("prefix");
    String entityName = "dataset";
    String expectedIndexName = "prefix_datasetindex_v2";
    assertEquals(indexConventionPrefix.getEntityIndexName(entityName), expectedIndexName);
    assertEquals(indexConventionPrefix.getPrefix(), Optional.of("prefix"));
    assertEquals(indexConventionPrefix.getEntityName(expectedIndexName), Optional.of(entityName));
    assertEquals(indexConventionPrefix.getEntityName("totally not an index"), Optional.empty());
    assertEquals(indexConventionPrefix.getEntityName("prefix_dataset_v2"), Optional.empty());
    assertEquals(indexConventionPrefix.getEntityName("prefix_dashboardindex_v2_1683649932260"), Optional.of("dashboard"));
    assertEquals(indexConventionPrefix.getEntityName("dashboardindex_v2_1683649932260"), Optional.empty());
  }
}
