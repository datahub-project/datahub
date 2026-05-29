package com.linkedin.datahub.graphql.resolvers.timeline;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.generated.GetTimelineInput;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import org.testng.annotations.Test;

public class GetTimelineResolverTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:kafka,test-timeline-dataset,PROD)";

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

  @Test
  public void testGetUnauthorizedThrowsAndDoesNotQueryDb() {
    TimelineService mockTimelineService = mock(TimelineService.class);
    GetTimelineResolver resolver = new GetTimelineResolver(mockTimelineService);

    QueryContext denyContext = getMockDenyContextWithOperationContext();

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(denyContext);

    GetTimelineInput input = new GetTimelineInput();
    input.setUrn(TEST_DATASET_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);

    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv));
    verifyNoInteractions(mockTimelineService);
  }

  @Test
  public void testGetAuthorizedReturnsResult() throws Exception {
    TimelineService mockTimelineService = mock(TimelineService.class);
    when(mockTimelineService.getTimeline(any(), any(), anyInt(), anyBoolean()))
        .thenReturn(List.of());

    GetTimelineResolver resolver = new GetTimelineResolver(mockTimelineService);

    QueryContext allowContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(allowContext);

    GetTimelineInput input = new GetTimelineInput();
    input.setUrn(TEST_DATASET_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);

    assertNotNull(resolver.get(mockEnv).get());
    verify(mockTimelineService, times(1)).getTimeline(any(), any(), anyInt(), anyBoolean());
  }
}
