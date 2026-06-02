package com.linkedin.metadata.utils;

import static org.testng.Assert.*;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import org.testng.annotations.Test;

public class UrnNormalizationUtilsTest {

  @Test
  public void testNormalizeDatasetUrn() {
    // Test dataset URN normalization
    DatasetUrn originalUrn =
        new DatasetUrn(new DataPlatformUrn("Snowflake"), "DB.SCHEMA.TABLE", FabricType.PROD);

    Urn normalizedUrn = UrnNormalizationUtils.normalizeUrn(originalUrn);

    // Verify normalization
    DatasetUrn normalized = (DatasetUrn) normalizedUrn;
    assertEquals(normalized.getPlatformEntity().getPlatformNameEntity(), "snowflake");
    assertEquals(normalized.getDatasetNameEntity(), "db.schema.table");
    assertEquals(normalized.getOriginEntity(), FabricType.PROD); // FabricType unchanged
  }

  @Test
  public void testNormalizeDatasetUrnMixedCase() {
    // Test with mixed case dataset name
    DatasetUrn originalUrn =
        new DatasetUrn(new DataPlatformUrn("MSSQL"), "MyDatabase.dbo.MyTable", FabricType.DEV);

    Urn normalizedUrn = UrnNormalizationUtils.normalizeUrn(originalUrn);

    DatasetUrn normalized = (DatasetUrn) normalizedUrn;
    assertEquals(normalized.getPlatformEntity().getPlatformNameEntity(), "mssql");
    assertEquals(normalized.getDatasetNameEntity(), "mydatabase.dbo.mytable");
    assertEquals(normalized.getOriginEntity(), FabricType.DEV);
  }

  @Test
  public void testNormalizeSchemaFieldUrn() throws Exception {
    // Test schema field URN normalization
    DatasetUrn parentDatasetUrn =
        new DatasetUrn(new DataPlatformUrn("BigQuery"), "PROJECT.DATASET.TABLE", FabricType.PROD);
    String fieldPath = "FieldName";

    Urn schemaFieldUrn = Urn.createFromTuple("schemaField", parentDatasetUrn.toString(), fieldPath);

    Urn normalizedUrn = UrnNormalizationUtils.normalizeUrn(schemaFieldUrn);

    // Parse normalized schema field URN
    String normalizedParentUrnString = normalizedUrn.getEntityKey().get(0);
    String normalizedFieldPath = normalizedUrn.getEntityKey().get(1);

    // Verify parent dataset URN is normalized
    DatasetUrn normalizedParent = DatasetUrn.createFromString(normalizedParentUrnString);
    assertEquals(normalizedParent.getPlatformEntity().getPlatformNameEntity(), "bigquery");
    assertEquals(normalizedParent.getDatasetNameEntity(), "project.dataset.table");
    assertEquals(normalizedParent.getOriginEntity(), FabricType.PROD);

    // Verify field path is normalized
    assertEquals(normalizedFieldPath, "fieldname");
  }

  @Test
  public void testNeedsNormalizationDataset() {
    // Test with uppercase dataset URN
    DatasetUrn uppercaseUrn =
        new DatasetUrn(new DataPlatformUrn("Snowflake"), "DB.TABLE", FabricType.PROD);
    assertTrue(UrnNormalizationUtils.needsNormalization(uppercaseUrn));

    // Test with lowercase dataset URN
    DatasetUrn lowercaseUrn =
        new DatasetUrn(new DataPlatformUrn("snowflake"), "db.table", FabricType.PROD);
    assertFalse(UrnNormalizationUtils.needsNormalization(lowercaseUrn));
  }

  @Test
  public void testNeedsNormalizationSchemaField() {
    // Test with uppercase schema field URN
    DatasetUrn parentUrn =
        new DatasetUrn(new DataPlatformUrn("mysql"), "db.TABLE", FabricType.PROD);
    Urn schemaFieldUrn = Urn.createFromTuple("schemaField", parentUrn.toString(), "FieldPath");
    assertTrue(UrnNormalizationUtils.needsNormalization(schemaFieldUrn));

    // Test with lowercase schema field URN
    DatasetUrn lowercaseParent =
        new DatasetUrn(new DataPlatformUrn("mysql"), "db.table", FabricType.PROD);
    Urn lowercaseSchemaFieldUrn =
        Urn.createFromTuple("schemaField", lowercaseParent.toString(), "fieldpath");
    assertFalse(UrnNormalizationUtils.needsNormalization(lowercaseSchemaFieldUrn));
  }

  @Test
  public void testAlreadyNormalized() {
    // Test dataset URN that is already lowercase
    DatasetUrn lowercaseUrn =
        new DatasetUrn(new DataPlatformUrn("postgres"), "schema.table", FabricType.PROD);

    assertFalse(UrnNormalizationUtils.needsNormalization(lowercaseUrn));

    // Normalizing should return equivalent URN
    Urn normalized = UrnNormalizationUtils.normalizeUrn(lowercaseUrn);
    assertEquals(normalized.toString(), lowercaseUrn.toString());
  }

  @Test
  public void testFabricTypePreserved() {
    // Test all FabricType values are preserved correctly
    FabricType[] fabricTypes = {
      FabricType.DEV,
      FabricType.PROD,
      FabricType.TEST,
      FabricType.QA,
      FabricType.STG,
      FabricType.SANDBOX
    };

    for (FabricType fabricType : fabricTypes) {
      DatasetUrn originalUrn =
          new DatasetUrn(new DataPlatformUrn("PLATFORM"), "DB.TABLE", fabricType);

      Urn normalizedUrn = UrnNormalizationUtils.normalizeUrn(originalUrn);
      DatasetUrn normalized = (DatasetUrn) normalizedUrn;

      // Verify FabricType is unchanged
      assertEquals(
          normalized.getOriginEntity(),
          fabricType,
          "FabricType should be preserved: " + fabricType);
    }
  }

  @Test
  public void testOtherEntityTypes() {
    // Test that other entity types are simply lowercased
    Urn corpUserUrn = UrnUtils.getUrn("urn:li:corpuser:JohnDoe");
    Urn normalized = UrnNormalizationUtils.normalizeUrn(corpUserUrn);

    assertEquals(normalized.toString(), "urn:li:corpuser:johndoe");
  }

  @Test
  public void testSpecialCharacters() {
    // Test URN with special characters in dataset name
    DatasetUrn urnWithSpecialChars =
        new DatasetUrn(
            new DataPlatformUrn("snowflake"), "DB.SCHEMA.\"Table-With-Dashes\"", FabricType.PROD);

    Urn normalized = UrnNormalizationUtils.normalizeUrn(urnWithSpecialChars);
    DatasetUrn normalizedDataset = (DatasetUrn) normalized;

    // Special characters should be preserved, only casing changed
    assertEquals(normalizedDataset.getDatasetNameEntity(), "db.schema.\"table-with-dashes\"");
  }

  @Test
  public void testIdempotence() {
    // Test that normalizing twice gives same result
    DatasetUrn originalUrn =
        new DatasetUrn(new DataPlatformUrn("BigQuery"), "PROJECT.DATASET.TABLE", FabricType.PROD);

    Urn normalized1 = UrnNormalizationUtils.normalizeUrn(originalUrn);
    Urn normalized2 = UrnNormalizationUtils.normalizeUrn(normalized1);

    assertEquals(normalized1.toString(), normalized2.toString());
  }
}
