package com.linkedin.datahub.graphql.resolvers.timeline;

import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.types.timeline.mappers.ChangeEventMapper;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class GetTimelineResolverTest {

  @Test
  public void testAllCategoryInputMapEntriesRoundTrip() {
    // For each (GraphQL -> Backend) entry in CATEGORY_INPUT_MAP, verify that
    // ChangeEventMapper maps the backend category back to the same GraphQL type.
    // This catches drift between the two maps.
    for (Map.Entry<ChangeCategoryType, ChangeCategory> entry :
        GetTimelineResolver.CATEGORY_INPUT_MAP.entrySet()) {
      ChangeCategoryType graphqlInput = entry.getKey();
      ChangeCategory backendCategory = entry.getValue();

      ChangeEvent testEvent =
          ChangeEvent.builder()
              .entityUrn("urn:li:dataset:test")
              .category(backendCategory)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description("round-trip test")
              .build();

      com.linkedin.datahub.graphql.generated.ChangeEvent mapped = ChangeEventMapper.map(testEvent);

      assertEquals(
          mapped.getCategory(),
          graphqlInput,
          "Round-trip mismatch: GraphQL "
              + graphqlInput
              + " -> backend "
              + backendCategory
              + " -> GraphQL "
              + mapped.getCategory());
    }
  }

  @Test
  public void testCategoryInputMapCoversAllBackendMappedCategories() {
    // Verify that CATEGORY_INPUT_MAP has the expected number of entries.
    // Update this count when new categories are added.
    assertEquals(
        GetTimelineResolver.CATEGORY_INPUT_MAP.size(),
        10,
        "CATEGORY_INPUT_MAP size changed — update this test and verify "
            + "ChangeEventMapper.CATEGORY_MAP is also updated");
  }

  @Test
  public void testOwnershipCategoryMapsToOwnerBackend() {
    // The OWNERSHIP -> OWNER mapping is critical because the enum names differ
    ChangeCategory result =
        GetTimelineResolver.CATEGORY_INPUT_MAP.get(ChangeCategoryType.OWNERSHIP);
    assertEquals(result, ChangeCategory.OWNER);
  }

  @Test
  public void testCategoryInputMapValuesAreDistinct() {
    // Verify no two GraphQL types map to the same backend category —
    // duplicate values would silently drop timeline events.
    Set<ChangeCategory> distinctValues =
        GetTimelineResolver.CATEGORY_INPUT_MAP.values().stream().collect(Collectors.toSet());
    assertEquals(
        distinctValues.size(),
        GetTimelineResolver.CATEGORY_INPUT_MAP.size(),
        "CATEGORY_INPUT_MAP has duplicate backend values");
  }
}
