package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class UrnSearchIdentityTest {

  private final EntityRegistry registry =
      TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();

  @Test
  public void testSimpleIdsKeepCaseAndDropUrnStructure() {
    assertEquals(identity("urn:li:tag:Confidential"), "Confidential");
    assertEquals(identity("urn:li:corpuser:alice"), "alice");
    assertEquals(identity("urn:li:glossaryTerm:Revenue"), "Revenue");
    assertFalse(identity("urn:li:tag:Confidential").contains("tag"));
  }

  @Test
  public void testDatasetNameOmitsPlatformAndFabric() {
    assertEquals(
        identity("urn:li:dataset:(urn:li:dataPlatform:hive,sales.orders,PROD)"), "sales.orders");
    assertEquals(
        identity(
            "urn:li:dataset:(urn:li:dataPlatform:s3,s3://bucket/year=2024/month=01/file.parquet,PROD)"),
        "s3://bucket/year=2024/month=01/file.parquet");
    assertEquals(
        identity(
            "urn:li:dataset:(urn:li:dataPlatform:abs,abfss://container@account.dfs.core.windows.net/path,PROD)"),
        "abfss://container@account.dfs.core.windows.net/path");
    assertEquals(
        identity("urn:li:dataset:(urn:li:dataPlatform:file,file:///tmp/my file.csv,PROD)"),
        "file:///tmp/my file.csv");
  }

  @Test
  public void testSchemaFieldPathOmitsParentAndAnnotations() {
    String parent = "urn:li:dataset:(urn:li:dataPlatform:hive,sales.orders,PROD)";
    assertEquals(
        identity("urn:li:schemaField:(" + parent + ",[version=2.0].[type=string].order_id)"),
        "order_id");
    assertEquals(
        identity("urn:li:schemaField:(" + parent + ",[version=2.0].[type=string].address.city)"),
        "address.city");
    assertEquals(
        identity("urn:li:schemaField:(" + parent + ",[type=array%28string%2Cint%29].order_id)"),
        "order_id");
    String column = identity("urn:li:schemaField:(" + parent + ",order_id)");
    assertFalse(column.contains("sales"));
    assertFalse(column.contains("hive"));
    assertFalse(column.contains("PROD"));
  }

  @Test
  public void testEncodedDatasetNameReducesAndLiteralPercentStays() {
    assertEquals(
        identity("urn:li:dataset:%28urn:li:dataPlatform:s3%2Cbucket/path%2CPROD%29"),
        "bucket/path");
    assertEquals(identity("urn:li:tag:a%28b%29c"), "a(b)c");
    assertEquals(UrnSearchIdentity.decodeReserved("% of pop"), "% of pop");
    assertFalse(UrnSearchIdentity.decodeReserved("100% organic").contains("%28"));
  }

  @Test
  public void testMalformedUrnContributesNothing() {
    assertEquals(identity("urn:li:dataset:not-a-tuple"), "");
    assertEquals(identity("urn:li:madeUp:value"), "");
  }

  @Test
  public void testDataFlowEnvironmentStaysInTheIdentity() {
    String identity = identity("urn:li:dataFlow:(airflow,nightly,PROD)");
    assertTrue(identity.contains("nightly"));
    assertTrue(identity.contains("PROD"));
  }

  private String identity(String urn) {
    return UrnSearchIdentity.identity(UrnUtils.getUrn(urn), registry);
  }
}
