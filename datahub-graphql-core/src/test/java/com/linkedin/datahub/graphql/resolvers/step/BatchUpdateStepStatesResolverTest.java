package com.linkedin.datahub.graphql.resolvers.step;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchUpdateStepStatesInput;
import com.linkedin.datahub.graphql.generated.BatchUpdateStepStatesResult;
import com.linkedin.datahub.graphql.generated.StepStateInput;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BatchUpdateStepStatesResolverTest {
  private static final Urn ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final String FIRST_STEP_STATE_ID = "1";
  private EntityClient _entityClient;
  private BatchUpdateStepStatesResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new BatchUpdateStepStatesResolver(_entityClient);
  }

  @Test
  public void testBatchUpdateStepStatesFirstStepCompleted() throws Exception {
    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_authentication.getActor()).thenReturn(new Actor(ActorType.USER, ACTOR_URN.toString()));

    final BatchUpdateStepStatesInput input = new BatchUpdateStepStatesInput();
    final StepStateInput firstInput = new StepStateInput();
    firstInput.setId(FIRST_STEP_STATE_ID);
    firstInput.setProperties(Collections.emptyList());
    input.setStates(ImmutableList.of(firstInput));
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    final BatchUpdateStepStatesResult actualBatchResult = _resolver.get(_dataFetchingEnvironment).join();
    assertNotNull(actualBatchResult);
    assertEquals(1, actualBatchResult.getResults().size());
    verify(_entityClient, times(1)).ingestProposal(any(), eq(_authentication), eq(false));
  }
}
