package com.linkedin.metadata.recommendation.candidatesource;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import org.junit.Assert;
import org.testng.annotations.Test;

public class RecommendationUtilsTest {

  @Test
  private void testIsSupportedEntityType() {
    Urn testUrn = UrnUtils.getUrn("urn:li:corpuser:john");
    Assert.assertTrue(
        RecommendationUtils.isSupportedEntityType(
            testUrn,
            ImmutableSet.of(Constants.DATASET_ENTITY_NAME, Constants.CORP_USER_ENTITY_NAME)));
    Assert.assertFalse(
        RecommendationUtils.isSupportedEntityType(
            testUrn, ImmutableSet.of(Constants.DATASET_ENTITY_NAME)));
    Assert.assertFalse(RecommendationUtils.isSupportedEntityType(testUrn, Collections.emptySet()));
  }
}
