package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.Locale;
import org.testng.annotations.Test;

public class AliasesUtilsTest {

  @Test
  public void testLowercasesEveryComponent() {
    Urn mixed =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:adlsGen2,Container/Folder,PROD)");
    assertEquals(
        AliasesUtils.lowercasedUrnKey(mixed),
        "urn:li:dataset:(urn:li:dataplatform:adlsgen2,container/folder,prod)");
  }

  @Test
  public void testLowercasesEmbeddedPlatformInstanceAsPartOfName() {
    Urn withInstance =
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Instance.DB.Schema.Table,PROD)");
    assertEquals(
        AliasesUtils.lowercasedUrnKey(withInstance),
        "urn:li:dataset:(urn:li:dataplatform:snowflake,my_instance.db.schema.table,prod)");
  }

  /** Distinct environments stay distinct: lowercasing does not collapse them onto one key. */
  @Test
  public void testKeepsEnvironmentsDistinct() {
    Urn prod = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)");
    Urn dev = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,DEV)");
    assertNotEquals(AliasesUtils.lowercasedUrnKey(prod), AliasesUtils.lowercasedUrnKey(dev));
  }

  /** One rule for every entity type, so the side effect needs no per-type derivation. */
  @Test
  public void testAppliesToNonDatasetUrns() {
    Urn chartUrn = UrnUtils.getUrn("urn:li:chart:(Looker,My_Chart)");
    assertEquals(AliasesUtils.lowercasedUrnKey(chartUrn), "urn:li:chart:(looker,my_chart)");
  }

  /** The column name is a real source of casing variance, and one rule reaches it. */
  @Test
  public void testLowercasesSchemaFieldColumnName() {
    Urn field =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.Schema.Table,PROD),ColumnName)");
    assertEquals(
        AliasesUtils.lowercasedUrnKey(field),
        "urn:li:schemafield:(urn:li:dataset:(urn:li:dataplatform:snowflake,db.schema.table,prod),columnname)");
  }

  /**
   * A Turkish default locale lowercases {@code I} to the dotless {@code ı}, so without {@code
   * Locale.ROOT} the key would depend on the JVM's locale and stop matching what clients compute.
   */
  @Test
  public void testLowercasesUnicodeIndependentOfDefaultLocale() {
    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr"));
      Urn unicodeName =
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,CAFÉ.Ñ_TITLE,PROD)");
      assertEquals(
          AliasesUtils.lowercasedUrnKey(unicodeName),
          "urn:li:dataset:(urn:li:dataplatform:snowflake,café.ñ_title,prod)");
    } finally {
      Locale.setDefault(original);
    }
  }

  @Test
  public void testIdempotent() {
    Urn mixed =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.Schema.Table,PROD)");
    String once = AliasesUtils.lowercasedUrnKey(mixed);
    assertEquals(AliasesUtils.lowercasedUrnKey(UrnUtils.getUrn(once)), once);
  }
}
