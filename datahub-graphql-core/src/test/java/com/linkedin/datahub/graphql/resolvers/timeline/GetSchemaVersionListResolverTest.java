package com.linkedin.datahub.graphql.resolvers.timeline;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetSchemaVersionListInput;
import com.linkedin.metadata.timeline.TimelineService;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import org.testng.annotations.Test;

public class GetSchemaVersionListResolverTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:kafka,test-schema-version-dataset,PROD)";

  @Test
  public void testGetUnauthorizedThrowsAndDoesNotQueryDb() throws Exception {
    TimelineService mockTimelineService = mock(TimelineService.class);
    GetSchemaVersionListResolver resolver = new GetSchemaVersionListResolver(mockTimelineService);

    QueryContext denyContext = getMockDenyContextWithOperationContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(denyContext);

    GetSchemaVersionListInput input = new GetSchemaVersionListInput();
    input.setDatasetUrn(TEST_DATASET_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);

    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv));
    verifyNoInteractions(mockTimelineService);
  }

  @Test
  public void testGetAuthorizedInvokesTimelineService() throws Exception {
    TimelineService mockTimelineService = mock(TimelineService.class);
    when(mockTimelineService.getTimeline(
            any(), any(), anyLong(), anyLong(), any(), any(), anyBoolean()))
        .thenReturn(Collections.emptyList());

    GetSchemaVersionListResolver resolver = new GetSchemaVersionListResolver(mockTimelineService);

    QueryContext allowContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(allowContext);

    GetSchemaVersionListInput input = new GetSchemaVersionListInput();
    input.setDatasetUrn(TEST_DATASET_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);

    resolver.get(mockEnv).get();
    verify(mockTimelineService, times(1))
        .getTimeline(any(), any(), anyLong(), anyLong(), any(), any(), anyBoolean());
  }
}
