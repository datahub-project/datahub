package com.linkedin.datahub.graphql.resolvers.timeline;

import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import org.testng.annotations.Test;

public class GetTimelineResolverTest {

  @Test
  public void testAllGraphqlCategoriesMatchBackendEnum() {
    // Since we removed the mapping tables, the GraphQL enum names must match the
    // backend ChangeCategory enum names exactly. Verify every GraphQL value can
    // be resolved via ChangeCategory.valueOf.
    for (ChangeCategoryType graphqlType : ChangeCategoryType.values()) {
      ChangeCategory backendCategory = ChangeCategory.valueOf(graphqlType.toString());
      assertNotNull(
          backendCategory,
          "GraphQL ChangeCategoryType."
              + graphqlType
              + " has no matching ChangeCategory enum value");
    }
  }

  @Test
  public void testOwnershipCategoryMatchesDirectly() {
    // Verify OWNERSHIP is now a direct match (no OWNER -> OWNERSHIP mapping needed)
    assertEquals(
        ChangeCategory.valueOf(ChangeCategoryType.OWNERSHIP.toString()), ChangeCategory.OWNERSHIP);
  }
}
