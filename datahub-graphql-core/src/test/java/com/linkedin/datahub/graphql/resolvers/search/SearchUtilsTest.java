package com.linkedin.datahub.graphql.resolvers.search;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.Constants;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SearchUtilsTest {

  @Test
  public static void testApplyViewToEntityTypes() {

    List<String> baseEntityTypes =
        ImmutableList.of(Constants.CHART_ENTITY_NAME, Constants.DATASET_ENTITY_NAME);

    List<String> viewEntityTypes =
        ImmutableList.of(Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME);

    final List<String> result = SearchUtils.intersectEntityTypes(baseEntityTypes, viewEntityTypes);

    final List<String> expectedResult = ImmutableList.of(Constants.DATASET_ENTITY_NAME);
    Assert.assertEquals(expectedResult, result);
  }

  private SearchUtilsTest() {}
}
