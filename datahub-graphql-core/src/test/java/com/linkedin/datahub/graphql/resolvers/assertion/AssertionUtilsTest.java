package com.linkedin.datahub.graphql.resolvers.assertion;

import static org.testng.Assert.assertEquals;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import org.testng.annotations.Test;

@SuppressWarnings("null")
public class AssertionUtilsTest {

  private static final Urn ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");

  @Test
  public void testGetAsserteeUrnFromDatasetAssertionInfo() {
    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(ENTITY_URN));

    assertEquals(AssertionUtils.getAsserteeUrnFromInfo(info), ENTITY_URN);
  }

  @Test
  public void testGetAsserteeUrnFromFreshnessAssertionInfo() {
    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.FRESHNESS)
            .setFreshnessAssertion(new FreshnessAssertionInfo().setEntity(ENTITY_URN));

    assertEquals(AssertionUtils.getAsserteeUrnFromInfo(info), ENTITY_URN);
  }

  @Test
  public void testGetAsserteeUrnFromVolumeAssertionInfo() {
    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.VOLUME)
            .setVolumeAssertion(new VolumeAssertionInfo().setEntity(ENTITY_URN));

    assertEquals(AssertionUtils.getAsserteeUrnFromInfo(info), ENTITY_URN);
  }

  @Test
  public void testGetAsserteeUrnFromSqlAssertionInfo() {
    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.SQL)
            .setSqlAssertion(new SqlAssertionInfo().setEntity(ENTITY_URN));

    assertEquals(AssertionUtils.getAsserteeUrnFromInfo(info), ENTITY_URN);
  }

  @Test
  public void testGetAsserteeUrnFromFieldAssertionInfo() {
    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.FIELD)
            .setFieldAssertion(new FieldAssertionInfo().setEntity(ENTITY_URN));

    assertEquals(AssertionUtils.getAsserteeUrnFromInfo(info), ENTITY_URN);
  }

  @Test
  public void testGetAsserteeUrnFromSchemaAssertionInfo() {
    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.DATA_SCHEMA)
            .setSchemaAssertion(new SchemaAssertionInfo().setEntity(ENTITY_URN));

    assertEquals(AssertionUtils.getAsserteeUrnFromInfo(info), ENTITY_URN);
  }

  @Test
  public void testGetAsserteeUrnFromCustomAssertionInfo() {
    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.CUSTOM)
            .setCustomAssertion(new CustomAssertionInfo().setEntity(ENTITY_URN));

    assertEquals(AssertionUtils.getAsserteeUrnFromInfo(info), ENTITY_URN);
  }
}
