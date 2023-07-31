package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AssertionUtilsTest {

  private static final Urn TEST_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
  private static final Urn TEST_DATAJOB_URN = UrnUtils.getUrn("urn:li:dataJob:test");

  @Test
  public void testGetAsserteeUrnFromInfo() {
    // Case 1: Dataset Assertion
    AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.DATASET);
    info.setDatasetAssertion(new DatasetAssertionInfo()
      .setOperator(AssertionStdOperator.IN)
      .setScope(DatasetAssertionScope.DATASET_COLUMN)
      .setDataset(TEST_DATASET_URN)
      .setAggregation(AssertionStdAggregation.MAX)
    );
    Urn result = AssertionUtils.getAsserteeUrnFromInfo(info);
    Assert.assertEquals(result, TEST_DATASET_URN);

    // Case 2: Dataset FRESHNESS Assertion
    info = new AssertionInfo();
    info.setType(AssertionType.FRESHNESS);
    info.setFreshnessAssertion(new FreshnessAssertionInfo()
        .setType(FreshnessAssertionType.DATASET_CHANGE)
        .setEntity(TEST_DATASET_URN)
    );
    result = AssertionUtils.getAsserteeUrnFromInfo(info);
    Assert.assertEquals(result, TEST_DATASET_URN);


    // Case 3: DataJob FRESHNESS Assertion
    info = new AssertionInfo();
    info.setType(AssertionType.FRESHNESS);
    info.setFreshnessAssertion(new FreshnessAssertionInfo()
        .setType(FreshnessAssertionType.DATASET_CHANGE)
        .setEntity(TEST_DATAJOB_URN)
    );
    result = AssertionUtils.getAsserteeUrnFromInfo(info);
    Assert.assertEquals(result, TEST_DATAJOB_URN);

    // Case 4: Unsupported Assertion Type
    final AssertionInfo badInfo = new AssertionInfo();
    info.setType(AssertionType.$UNKNOWN);
    info.setFreshnessAssertion(new FreshnessAssertionInfo()
        .setType(FreshnessAssertionType.DATA_JOB_RUN)
        .setEntity(TEST_DATASET_URN)
    );
    Assert.assertThrows(RuntimeException.class, () -> AssertionUtils.getAsserteeUrnFromInfo(badInfo));
  }
}