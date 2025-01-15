package com.linkedin.metadata.entity.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

public class ValidationApiUtilsTest {
  private static final EntityRegistry entityRegistry =
      TestOperationContexts.defaultEntityRegistry();

  @Test
  public void testValidateDatasetUrn() {
    Urn validUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD)");
    ValidationApiUtils.validateUrn(entityRegistry, validUrn, true);
    // If no exception is thrown, test passes
  }

  @Test
  public void testSimpleUrnColon() {
    ValidationApiUtils.validateUrn(
        entityRegistry, UrnUtils.getUrn("urn:li:corpuser:foo:bar"), true);
    ValidationApiUtils.validateUrn(
        entityRegistry, UrnUtils.getUrn("urn:li:dataPlatform:abc:def"), true);
    ValidationApiUtils.validateUrn(
        entityRegistry, UrnUtils.getUrn("urn:li:corpuser:foo:bar@example.com"), true);
    // If no exception is thrown, test passes
  }

  @Test
  public void testSimpleUrnComma() {
    ValidationApiUtils.validateUrn(entityRegistry, UrnUtils.getUrn("urn:li:corpuser:,"), true);
    // If no exception is thrown, test passes
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testTupleUrnComma() {
    ValidationApiUtils.validateUrn(
        entityRegistry, UrnUtils.getUrn("urn:li:dashboard:(looker,dashboards,thelook)"), true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFabricTypeCasing() {
    // prod != PROD
    ValidationApiUtils.validateUrn(
        entityRegistry,
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:abc:def,table_name,prod)"),
        true);
  }

  @Test
  public void testComplexUrnColon() throws URISyntaxException {
    Urn validUrn =
        Urn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:s3,urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29,PROD)");
    ValidationApiUtils.validateUrn(entityRegistry, validUrn, true);
    // If no exception is thrown, test passes
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFabricTypeParen() {
    Urn invalidUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,())");
    ValidationApiUtils.validateUrn(entityRegistry, invalidUrn, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testUrnWithTrailingWhitespace() {
    Urn invalidUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,/path/to/data,PROD) ");
    ValidationApiUtils.validateUrn(entityRegistry, invalidUrn, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testUrnWithIllegalDelimiter() {
    Urn invalidUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs␟path,PROD)");
    ValidationApiUtils.validateUrn(entityRegistry, invalidUrn, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testComplexUrnWithParens1() {
    Urn invalidUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,(illegal),PROD)");
    ValidationApiUtils.validateUrn(entityRegistry, invalidUrn, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testComplexUrnWithParens2() {
    Urn invalidUrn =
        UrnUtils.getUrn(
            "urn:li:dataJob:(urn:li:dataFlow:(mssql,1/2/3/4.c_n on %28LOCAL%29,PROD),1/2/3/4.c_n on (LOCAL))");
    ValidationApiUtils.validateUrn(entityRegistry, invalidUrn, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSimpleUrnWithParens() {
    Urn invalidUrn = UrnUtils.getUrn("urn:li:corpuser:(foo)123");
    ValidationApiUtils.validateUrn(entityRegistry, invalidUrn, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testExcessiveLength() {
    StringBuilder longPath = new StringBuilder("urn:li:dataset:(urn:li:dataPlatform:hdfs,");
    // Create a path that will exceed 512 bytes when URL encoded
    for (int i = 0; i < 500; i++) {
      longPath.append("very/long/path/");
    }
    longPath.append(",PROD)");
    Urn invalidUrn = UrnUtils.getUrn(longPath.toString());

    ValidationApiUtils.validateUrn(entityRegistry, invalidUrn, true);
  }

  @Test
  public void testValidComplexUrn() {
    Urn validUrn =
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,myproject.dataset.table,PROD)");

    ValidationApiUtils.validateUrn(entityRegistry, validUrn, true);
    // If no exception is thrown, test passes
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testUrnNull() {
    ValidationApiUtils.validateUrn(entityRegistry, null, true);
  }

  @Test
  public void testValidPartialUrlEncode() {
    Urn validUrn = UrnUtils.getUrn("urn:li:assertion:123=-%28__% weekly__%29");

    ValidationApiUtils.validateUrn(entityRegistry, validUrn, true);
    // If no exception is thrown, test passes
  }

  @Test
  public void testValidPartialUrlEncode2() {
    Urn validUrn =
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:s3,urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts%prog_maintenance%2CPROD%29,PROD)");

    ValidationApiUtils.validateUrn(entityRegistry, validUrn, true);
    // If no exception is thrown, test passes
  }

  @Test
  public void testValidColon() {
    Urn validUrn =
        UrnUtils.getUrn("urn:li:dashboard:(looker,dashboards.thelook::cohort_data_tool)");

    ValidationApiUtils.validateUrn(entityRegistry, validUrn, true);
    // If no exception is thrown, test passes
  }

  @Test
  public void testNoTupleComma() {
    Urn invalidUrn = UrnUtils.getUrn("urn:li:corpuser:,");
    ValidationApiUtils.validateUrn(entityRegistry, invalidUrn, true);
    // If no exception is thrown, test passes
  }
}
