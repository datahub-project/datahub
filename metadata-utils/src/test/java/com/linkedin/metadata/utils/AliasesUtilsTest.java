package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.net.URISyntaxException;
import java.util.Locale;
import org.testng.annotations.Test;

public class AliasesUtilsTest {

  @Test
  public void testPreservesPlatformCasing() throws URISyntaxException {
    Urn mixedPlatform =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:adlsGen2,Container/Folder,PROD)");
    assertEquals(
        AliasesUtils.lowercaseDatasetUrn(mixedPlatform).toString(),
        "urn:li:dataset:(urn:li:dataPlatform:adlsGen2,container/folder,PROD)");
  }

  @Test
  public void testLowercasesName() throws URISyntaxException {
    Urn mixedName =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.Schema.Table,PROD)");
    assertEquals(
        AliasesUtils.lowercaseDatasetUrn(mixedName).toString(),
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)");
  }

  @Test
  public void testPreservesEnv() throws URISyntaxException {
    Urn dev = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,DEV)");
    assertEquals(AliasesUtils.lowercaseDatasetUrn(dev).getOriginEntity(), FabricType.DEV);
    assertEquals(AliasesUtils.lowercaseDatasetUrn(dev).toString(), dev.toString());
  }

  @Test
  public void testLowercasesEmbeddedPlatformInstanceAsPartOfName() throws URISyntaxException {
    Urn withInstance =
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Instance.DB.Schema.Table,PROD)");
    assertEquals(
        AliasesUtils.lowercaseDatasetUrn(withInstance).toString(),
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_instance.db.schema.table,PROD)");
  }

  /**
   * A Turkish default locale lowercases {@code I} to the dotless {@code ı}, so without {@code
   * Locale.ROOT} the key would depend on the JVM's locale and stop matching what clients compute.
   */
  @Test
  public void testLowercasesUnicodeNameIndependentOfDefaultLocale() throws URISyntaxException {
    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr"));
      Urn unicodeName =
          UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,CAFÉ.Ñ_TITLE,PROD)");
      assertEquals(
          AliasesUtils.lowercaseDatasetUrn(unicodeName).toString(),
          "urn:li:dataset:(urn:li:dataPlatform:snowflake,café.ñ_title,PROD)");
    } finally {
      Locale.setDefault(original);
    }
  }

  @Test
  public void testIdempotentOnAlreadyLowercased() throws URISyntaxException {
    Urn lower =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)");
    assertEquals(AliasesUtils.lowercaseDatasetUrn(lower).toString(), lower.toString());
  }

  @Test
  public void testNonDatasetUrnRejected() {
    Urn chartUrn = UrnUtils.getUrn("urn:li:chart:(looker,my_chart)");
    assertThrows(URISyntaxException.class, () -> AliasesUtils.lowercaseDatasetUrn(chartUrn));
  }
}
