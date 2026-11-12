package com.linkedin.datahub.graphql.resolvers.step;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchUpdateStepStatesInput;
import com.linkedin.datahub.graphql.generated.BatchUpdateStepStatesResult;
import com.linkedin.datahub.graphql.generated.StepStateInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpdateStepStateResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchUpdateStepStatesResolverTest {
  private static final Urn ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final String FIRST_STEP_STATE_ID = "1";
  private static final String SECOND_STEP_STATE_ID = "2";
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
    setupMockContext();

    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(buildInput(FIRST_STEP_STATE_ID));

    final BatchUpdateStepStatesResult actualBatchResult =
        _resolver.get(_dataFetchingEnvironment).join();
    assertNotNull(actualBatchResult);
    assertEquals(1, actualBatchResult.getResults().size());
    assertTrue(actualBatchResult.getResults().get(0).getSucceeded());
    verify(_entityClient, times(1)).batchIngestProposals(any(), anyCollection(), eq(false));
  }

  @Test
  public void testBatchUpdateStepStatesIngestsAllStatesInOneBatch() throws Exception {
    setupMockContext();

    when(_dataFetchingEnvironment.getArgument("input"))
        .thenReturn(buildInput(FIRST_STEP_STATE_ID, SECOND_STEP_STATE_ID));

    final BatchUpdateStepStatesResult actualBatchResult =
        _resolver.get(_dataFetchingEnvironment).join();
    assertNotNull(actualBatchResult);
    assertEquals(2, actualBatchResult.getResults().size());
    assertTrue(
        actualBatchResult.getResults().stream().allMatch(UpdateStepStateResult::getSucceeded));

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Collection<MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(Collection.class);
    verify(_entityClient, times(1))
        .batchIngestProposals(any(), proposalsCaptor.capture(), eq(false));
    assertEquals(2, proposalsCaptor.getValue().size());
    verify(_entityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testBatchUpdateStepStatesFallsBackToIndividualUpdatesOnBatchFailure()
      throws Exception {
    setupMockContext();

    when(_dataFetchingEnvironment.getArgument("input"))
        .thenReturn(buildInput(FIRST_STEP_STATE_ID, SECOND_STEP_STATE_ID));

    when(_entityClient.batchIngestProposals(any(), anyCollection(), eq(false)))
        .thenThrow(new RuntimeException("batch failed"));
    // The first state is retried successfully, the second keeps failing.
    when(_entityClient.ingestProposal(any(), any(), eq(false)))
        .thenReturn(FIRST_STEP_STATE_ID)
        .thenThrow(new RuntimeException("individual failure"));

    final BatchUpdateStepStatesResult actualBatchResult =
        _resolver.get(_dataFetchingEnvironment).join();
    assertNotNull(actualBatchResult);
    assertEquals(2, actualBatchResult.getResults().size());
    assertTrue(actualBatchResult.getResults().get(0).getSucceeded());
    assertFalse(actualBatchResult.getResults().get(1).getSucceeded());
    verify(_entityClient, times(2)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testBatchUpdateStepStatesReportsMalformedStateWithoutFailingTheBatch()
      throws Exception {
    setupMockContext();

    // Duplicate property keys cannot be collected into a map, so this state cannot be converted
    // into a proposal. It must fail on its own rather than taking the whole mutation down.
    final BatchUpdateStepStatesInput input = new BatchUpdateStepStatesInput();
    input.setStates(
        Arrays.asList(
            buildState(FIRST_STEP_STATE_ID, entry("k", "1")),
            buildState(SECOND_STEP_STATE_ID, entry("k", "1"), entry("k", "2"))));
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    final BatchUpdateStepStatesResult actualBatchResult =
        _resolver.get(_dataFetchingEnvironment).join();
    assertNotNull(actualBatchResult);
    assertEquals(2, actualBatchResult.getResults().size());
    assertTrue(actualBatchResult.getResults().get(0).getSucceeded());
    assertFalse(actualBatchResult.getResults().get(1).getSucceeded());

    // Only the well-formed state is handed to the batch.
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Collection<MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(Collection.class);
    verify(_entityClient, times(1))
        .batchIngestProposals(any(), proposalsCaptor.capture(), eq(false));
    assertEquals(1, proposalsCaptor.getValue().size());
  }

  @Test
  public void testBatchUpdateStepStatesSkipsIngestWhenEveryStateIsMalformed() throws Exception {
    setupMockContext();

    final BatchUpdateStepStatesInput input = new BatchUpdateStepStatesInput();
    input.setStates(
        Collections.singletonList(
            buildState(FIRST_STEP_STATE_ID, entry("k", "1"), entry("k", "2"))));
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    final BatchUpdateStepStatesResult actualBatchResult =
        _resolver.get(_dataFetchingEnvironment).join();
    assertNotNull(actualBatchResult);
    assertEquals(1, actualBatchResult.getResults().size());
    assertFalse(actualBatchResult.getResults().get(0).getSucceeded());

    // Nothing was ingestable, so no request should have been made at all.
    verify(_entityClient, never()).batchIngestProposals(any(), anyCollection(), anyBoolean());
    verify(_entityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  private void setupMockContext() {
    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_authentication.getActor()).thenReturn(new Actor(ActorType.USER, ACTOR_URN.toString()));
  }

  private static BatchUpdateStepStatesInput buildInput(final String... ids) {
    final BatchUpdateStepStatesInput input = new BatchUpdateStepStatesInput();
    input.setStates(
        Arrays.stream(ids)
            .map(BatchUpdateStepStatesResolverTest::buildState)
            .collect(Collectors.toList()));
    return input;
  }

  private static StepStateInput buildState(
      final String id, final StringMapEntryInput... properties) {
    final StepStateInput stepStateInput = new StepStateInput();
    stepStateInput.setId(id);
    stepStateInput.setProperties(Arrays.asList(properties));
    return stepStateInput;
  }

  private static StringMapEntryInput entry(final String key, final String value) {
    final StringMapEntryInput entry = new StringMapEntryInput();
    entry.setKey(key);
    entry.setValue(value);
    return entry;
  }
}
