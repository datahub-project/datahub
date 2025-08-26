package com.linkedin.metadata.service.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchBasedFormAssignmentManagerTest {

  private SystemEntityClient mockEntityClient;
  private OperationContext mockOpContext;
  private FormToEntitiesConsumer<OperationContext, List<Urn>, Urn> mockConsumer;
  private static final String TEST_PREDICATE_JSON = "{\"filter\": \"test\"}";
  private static final int BATCH_SIZE = 10;
  private static final Urn TEST_FORM_URN = UrnUtils.getUrn("urn:li:form:test");
  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:dataset:(1,2,3)");

  @BeforeMethod
  public void setup() {
    mockEntityClient = mock(SystemEntityClient.class);
    mockOpContext = mock(OperationContext.class);
    mockConsumer = mock(FormToEntitiesConsumer.class);

    // Setup OperationContext mock to return itself when withSearchFlags is called
    when(mockOpContext.withSearchFlags(any())).thenReturn(mockOpContext);
  }

  @Test
  public void testApplyWithNoResults() throws Exception {
    // Setup
    ScrollResult emptyResult = new ScrollResult();
    emptyResult.setNumEntities(0);
    emptyResult.setEntities(new SearchEntityArray());
    when(mockEntityClient.scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            any(),
            anyString(),
            anyList(),
            anyInt(),
            anyList(),
            anyString()))
        .thenReturn(emptyResult);

    // Execute
    SearchBasedFormAssignmentManager.apply(
        mockOpContext,
        TEST_PREDICATE_JSON,
        TEST_FORM_URN,
        BATCH_SIZE,
        mockEntityClient,
        mockConsumer,
        true);

    // Verify
    verify(mockEntityClient, times(1))
        .scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            any(),
            anyString(),
            anyList(),
            anyInt(),
            anyList(),
            anyString());
    verify(mockConsumer, times(0)).accept(any(), any(), any());
  }

  @Test
  public void testApplyWithSingleBatch() throws Exception {
    // Setup
    List<SearchEntity> searchEntities = new ArrayList<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      SearchEntity entity = new SearchEntity();
      entity.setEntity(TEST_URN);
      searchEntities.add(entity);
    }

    ScrollResult result = new ScrollResult();
    result.setNumEntities(BATCH_SIZE);
    result.setEntities(new SearchEntityArray(searchEntities));
    //    result.setScrollId(null);

    when(mockEntityClient.scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            any(),
            anyString(),
            anyList(),
            anyInt(),
            anyList(),
            anyString()))
        .thenReturn(result);

    // Execute
    SearchBasedFormAssignmentManager.apply(
        mockOpContext,
        TEST_PREDICATE_JSON,
        TEST_FORM_URN,
        BATCH_SIZE,
        mockEntityClient,
        mockConsumer,
        true);

    // Verify
    verify(mockEntityClient, times(1))
        .scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            any(),
            anyString(),
            anyList(),
            anyInt(),
            anyList(),
            anyString());
    verify(mockConsumer, times(1)).accept(eq(mockOpContext), any(), eq(TEST_FORM_URN));
    verify(mockEntityClient, times(1)).ingestProposal(any(OperationContext.class), any(), eq(true));
  }

  @Test
  public void testApplyWithMultipleBatches() throws Exception {
    // Setup
    List<SearchEntity> firstBatch = new ArrayList<>();
    List<SearchEntity> secondBatch = new ArrayList<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      SearchEntity entity = new SearchEntity();
      entity.setEntity(TEST_URN);
      firstBatch.add(entity);
      secondBatch.add(entity);
    }

    ScrollResult firstResult = new ScrollResult();
    firstResult.setNumEntities(BATCH_SIZE);
    firstResult.setEntities(new SearchEntityArray(firstBatch));
    firstResult.setScrollId("scroll1");

    ScrollResult secondResult = new ScrollResult();
    secondResult.setNumEntities(BATCH_SIZE);
    secondResult.setEntities(new SearchEntityArray(secondBatch));
    //    secondResult.setScrollId(null);

    when(mockEntityClient.scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            any(),
            anyString(),
            anyList(),
            anyInt(),
            anyList(),
            anyString()))
        .thenReturn(firstResult)
        .thenReturn(secondResult);

    // Execute
    SearchBasedFormAssignmentManager.apply(
        mockOpContext,
        TEST_PREDICATE_JSON,
        TEST_FORM_URN,
        BATCH_SIZE,
        mockEntityClient,
        mockConsumer,
        true);

    // Verify
    verify(mockEntityClient, times(2))
        .scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            any(),
            anyString(),
            anyList(),
            anyInt(),
            anyList(),
            anyString());
    verify(mockConsumer, times(2)).accept(eq(mockOpContext), any(), eq(TEST_FORM_URN));
    verify(mockEntityClient, times(1)).ingestProposal(any(OperationContext.class), any(), eq(true));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testApplyWithRemoteInvocationException() throws Exception {
    // Setup
    when(mockEntityClient.scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            any(),
            anyString(),
            anyList(),
            anyInt(),
            anyList(),
            anyString()))
        .thenThrow(new RemoteInvocationException("Test exception"));

    // Execute
    SearchBasedFormAssignmentManager.apply(
        mockOpContext,
        TEST_PREDICATE_JSON,
        TEST_FORM_URN,
        BATCH_SIZE,
        mockEntityClient,
        mockConsumer,
        true);
  }

  @Test
  public void testApplyWithUnassigning() throws Exception {
    // Setup
    List<SearchEntity> searchEntities = new ArrayList<>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      SearchEntity entity = new SearchEntity();
      entity.setEntity(TEST_URN);
      searchEntities.add(entity);
    }

    ScrollResult result = new ScrollResult();
    result.setNumEntities(BATCH_SIZE);
    result.setEntities(new SearchEntityArray(searchEntities));
    //    result.setScrollId(null);

    when(mockEntityClient.scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            any(),
            anyString(),
            anyList(),
            anyInt(),
            anyList(),
            anyString()))
        .thenReturn(result);

    // Execute
    SearchBasedFormAssignmentManager.apply(
        mockOpContext,
        TEST_PREDICATE_JSON,
        TEST_FORM_URN,
        BATCH_SIZE,
        mockEntityClient,
        mockConsumer,
        false);

    // Verify
    verify(mockEntityClient, times(1))
        .scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            any(),
            anyString(),
            anyList(),
            anyInt(),
            anyList(),
            anyString());
    verify(mockConsumer, times(1)).accept(eq(mockOpContext), any(), eq(TEST_FORM_URN));
    verify(mockEntityClient, times(0))
        .ingestProposal(any(OperationContext.class), any(), anyBoolean());
  }
}
