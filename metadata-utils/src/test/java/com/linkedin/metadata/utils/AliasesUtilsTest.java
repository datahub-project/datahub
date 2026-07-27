package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

public class AliasesUtilsTest {

  @Test
  public void testLowercasesPlatformAndNameKeepsEnv() throws URISyntaxException {
    Urn mixed =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:Snowflake,DB.Schema.Table,DEV)");
    DatasetUrn lowercased = AliasesUtils.lowercaseDatasetUrn(mixed);
    assertEquals(
        lowercased.toString(),
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,DEV)");
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
