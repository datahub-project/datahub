package datahub.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import datahub.spark.model.dataset.CatalogTableDataset;
import org.apache.spark.sql.catalyst.plans.logical.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.catalyst.plans.logical.CreateHiveTableAsSelectCommand;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoHiveTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for DatasetExtractor helper methods including CTAS dedup detection and catalog table
 * name normalization.
 */
public class DatasetExtractorTest {

  @Test
  public void testStripDefaultCatalogPrefix() {
    // Test: default catalog prefix should be stripped
    String result = testStripDefaultCatalog("spark_catalog.default.table1");
    assertEquals("default.table1", result);
  }

  @Test
  public void testStripDefaultCatalogPrefixOnlyDefaultCatalog() {
    // Test: only "spark_catalog." prefix is stripped, not other catalogs
    String result = testStripDefaultCatalog("spark_catalog.table1");
    assertEquals("table1", result);
  }

  @Test
  public void testStripDefaultCatalogPreserveOtherCatalogs() {
    // Test: other catalog prefixes should be preserved
    String result = testStripDefaultCatalog("my_catalog.schema.table1");
    assertEquals("my_catalog.schema.table1", result);
  }

  @Test
  public void testStripDefaultCatalogNullInput() {
    // Test: null input should return null
    String result = testStripDefaultCatalog(null);
    assertEquals(null, result);
  }

  @Test
  public void testStripDefaultCatalogEmptyString() {
    // Test: empty string should return empty
    String result = testStripDefaultCatalog("");
    assertEquals("", result);
  }

  @Test
  public void testStripDefaultCatalogNoPrefix() {
    // Test: table name without prefix should remain unchanged
    String result = testStripDefaultCatalog("default.table1");
    assertEquals("default.table1", result);
  }

  @Test
  public void testIsCreateTableAsSelectCommand_CreateDataSourceCommand() {
    // Test: CreateDataSourceTableAsSelectCommand should be recognized
    LogicalPlan plan = Mockito.mock(CreateDataSourceTableAsSelectCommand.class);
    assertTrue(DatasetExtractor.isCreateTableAsSelectCommand(plan));
  }

  @Test
  public void testIsCreateTableAsSelectCommand_CreateHiveCommand() {
    // Test: CreateHiveTableAsSelectCommand should be recognized
    LogicalPlan plan = Mockito.mock(CreateHiveTableAsSelectCommand.class);
    assertTrue(DatasetExtractor.isCreateTableAsSelectCommand(plan));
  }

  @Test
  public void testIsCreateTableAsSelectCommand_OtherPlan() {
    // Test: other LogicalPlans should not be recognized as CTAS
    LogicalPlan plan = Mockito.mock(LogicalPlan.class);
    assertFalse(DatasetExtractor.isCreateTableAsSelectCommand(plan));
  }

  @Test
  public void testIsFollowUpInsertCommand_InsertIntoHadoopFsRelation() {
    // Test: InsertIntoHadoopFsRelationCommand should be recognized as follow-up insert
    LogicalPlan plan = Mockito.mock(InsertIntoHadoopFsRelationCommand.class);
    assertTrue(DatasetExtractor.isFollowUpInsertCommand(plan));
  }

  @Test
  public void testIsFollowUpInsertCommand_InsertIntoHiveTable() {
    // Test: InsertIntoHiveTable should be recognized as follow-up insert
    LogicalPlan plan = Mockito.mock(InsertIntoHiveTable.class);
    assertTrue(DatasetExtractor.isFollowUpInsertCommand(plan));
  }

  @Test
  public void testIsFollowUpInsertCommand_OtherPlan() {
    // Test: other LogicalPlans should not be recognized as follow-up insert
    LogicalPlan plan = Mockito.mock(LogicalPlan.class);
    assertFalse(DatasetExtractor.isFollowUpInsertCommand(plan));
  }

  /**
   * Helper method to test stripDefaultCatalog via reflection. The method is private static, so we
   * access it via reflection.
   */
  private static String testStripDefaultCatalog(String name) {
    try {
      java.lang.reflect.Method method =
          CatalogTableDataset.class.getDeclaredMethod("stripDefaultCatalog", String.class);
      method.setAccessible(true);
      return (String) method.invoke(null, name);
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke stripDefaultCatalog", e);
    }
  }
}
