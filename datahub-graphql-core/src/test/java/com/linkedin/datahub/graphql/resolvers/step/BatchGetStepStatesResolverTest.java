package com.linkedin.datahub.graphql.resolvers.step;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchGetStepStatesInput;
import com.linkedin.datahub.graphql.generated.BatchGetStepStatesResult;
import com.linkedin.datahub.graphql.resolvers.test.TestUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.step.DataHubStepStateProperties;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BatchGetStepStatesResolverTest {
  private static final Urn ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final long TIME = 123L;
  private static final AuditStamp AUDIT_STAMP = new AuditStamp().setActor(ACTOR_URN).setTime(TIME);
  private static final String FIRST_STEP_STATE_ID = "1";
  private static final String SECOND_STEP_STATE_ID = "2";
  private static final Urn FIRST_STEP_STATE_URN = UrnUtils.getUrn("urn:li:dataHubStepState:1");
  private static final Urn SECOND_STEP_STATE_URN = UrnUtils.getUrn("urn:li:dataHubStepState:2");
  private static final Set<String> ASPECTS = ImmutableSet.of(DATAHUB_STEP_STATE_PROPERTIES_ASPECT_NAME);
  private EntityClient _entityClient;
  private BatchGetStepStatesResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {

    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new BatchGetStepStatesResolver(_entityClient);
  }

  @Test
  public void testBatchGetStepStatesFirstStepCompleted() throws Exception {
    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    final BatchGetStepStatesInput input = new BatchGetStepStatesInput();
    input.setIds(ImmutableList.of(FIRST_STEP_STATE_ID, SECOND_STEP_STATE_ID));
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    when(_entityClient.exists(eq(FIRST_STEP_STATE_URN), eq(_authentication))).thenReturn(true);
    when(_entityClient.exists(eq(SECOND_STEP_STATE_URN), eq(_authentication))).thenReturn(false);

    final DataHubStepStateProperties firstStepStateProperties =
        new DataHubStepStateProperties().setLastModified(AUDIT_STAMP);

    final Set<Urn> urns = ImmutableSet.of(FIRST_STEP_STATE_URN);
    final Map<String, RecordTemplate> firstAspectMap = ImmutableMap.of(DATAHUB_STEP_STATE_PROPERTIES_ASPECT_NAME,
        firstStepStateProperties);
    final Map<Urn, EntityResponse> entityResponseMap = ImmutableMap.of(FIRST_STEP_STATE_URN,
        TestUtils.buildEntityResponse(firstAspectMap));

    when(_entityClient.batchGetV2(eq(DATAHUB_STEP_STATE_ENTITY_NAME), eq(urns), eq(ASPECTS), eq(_authentication)))
        .thenReturn(entityResponseMap);

    final BatchGetStepStatesResult actualBatchResult = _resolver.get(_dataFetchingEnvironment).join();
    assertNotNull(actualBatchResult);
    assertEquals(1, actualBatchResult.getResults().size());
  }

  @Test
  public void testBatchGetStepStatesBothStepsCompleted() throws Exception {
    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    final BatchGetStepStatesInput input = new BatchGetStepStatesInput();
    input.setIds(ImmutableList.of(FIRST_STEP_STATE_ID, SECOND_STEP_STATE_ID));
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    when(_entityClient.exists(eq(FIRST_STEP_STATE_URN), eq(_authentication))).thenReturn(true);
    when(_entityClient.exists(eq(SECOND_STEP_STATE_URN), eq(_authentication))).thenReturn(true);

    final DataHubStepStateProperties firstStepStateProperties =
        new DataHubStepStateProperties().setLastModified(AUDIT_STAMP);
    final DataHubStepStateProperties secondStepStateProperties =
        new DataHubStepStateProperties().setLastModified(AUDIT_STAMP);

    final Set<Urn> urns = ImmutableSet.of(FIRST_STEP_STATE_URN, SECOND_STEP_STATE_URN);
    final Map<String, RecordTemplate> firstAspectMap = ImmutableMap.of(DATAHUB_STEP_STATE_PROPERTIES_ASPECT_NAME,
        firstStepStateProperties);
    final Map<String, RecordTemplate> secondAspectMap = ImmutableMap.of(DATAHUB_STEP_STATE_PROPERTIES_ASPECT_NAME,
        secondStepStateProperties);
    final Map<Urn, EntityResponse> entityResponseMap = ImmutableMap.of(
        FIRST_STEP_STATE_URN, TestUtils.buildEntityResponse(firstAspectMap),
        SECOND_STEP_STATE_URN, TestUtils.buildEntityResponse(secondAspectMap));

    when(_entityClient.batchGetV2(eq(DATAHUB_STEP_STATE_ENTITY_NAME), eq(urns), eq(ASPECTS), eq(_authentication)))
        .thenReturn(entityResponseMap);

    final BatchGetStepStatesResult actualBatchResult = _resolver.get(_dataFetchingEnvironment).join();
    assertNotNull(actualBatchResult);
    assertEquals(2, actualBatchResult.getResults().size());
  }
}
