package com.linkedin.metadata.recommendation.candidatesource;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import org.junit.Assert;
import org.testng.annotations.Test;

public class RecommendationUtilsTest {

  @Test
  private void testIsSupportedEntityType() {
    Urn testUrn = UrnUtils.getUrn("urn:li:corpuser:john");
    OperationContext opContext = TestOperationContexts.userContextNoSearchAuthorization(testUrn);

    Assert.assertTrue(
        RecommendationUtils.isSupportedEntityType(
            opContext,
            ImmutableSet.of(Constants.DATASET_ENTITY_NAME, Constants.CORP_USER_ENTITY_NAME)));
    Assert.assertFalse(
        RecommendationUtils.isSupportedEntityType(
            opContext, ImmutableSet.of(Constants.DATASET_ENTITY_NAME)));
    Assert.assertFalse(
        RecommendationUtils.isSupportedEntityType(opContext, Collections.emptySet()));
  }
}
