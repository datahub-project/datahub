package datahub.spark;

import static org.junit.Assert.assertEquals;

import datahub.spark.model.dataset.CatalogTableDataset;
import org.junit.Test;

/** Unit tests for CatalogTableDataset catalog name normalization. */
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
