package com.linkedin.datahub.graphql.resolvers.knowledge;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.DocumentChange;
import com.linkedin.datahub.graphql.generated.DocumentChangeType;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DocumentChangeHistoryResolverTest {

  private static final Urn TEST_DOCUMENT_URN = UrnUtils.getUrn("urn:li:document:test-doc");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");

  private TimelineService mockTimelineService;
  private DocumentChangeHistoryResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private QueryContext mockContext;
  private Document sourceDocument;

  @BeforeMethod
  public void setupTest() {
    mockTimelineService = mock(TimelineService.class);
    mockEnv = mock(DataFetchingEnvironment.class);
    mockContext = mock(QueryContext.class);

    resolver = new DocumentChangeHistoryResolver(mockTimelineService);

    // Setup source document
    sourceDocument = new Document();
    sourceDocument.setUrn(TEST_DOCUMENT_URN.toString());

    when(mockEnv.getSource()).thenReturn(sourceDocument);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
  }

  @Test
  public void testGetChangeHistorySuccess() throws Exception {
    // Setup timeline service to return change events
    List<ChangeTransaction> transactions = new ArrayList<>();

    // Create a document creation event
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(TEST_USER_URN);

    ChangeEvent createEvent =
        ChangeEvent.builder()
            .category(ChangeCategory.LIFECYCLE)
            .operation(ChangeOperation.CREATE)
            .entityUrn(TEST_DOCUMENT_URN.toString())
            .description("Document 'Test Doc' was created")
            .auditStamp(auditStamp)
            .build();

    ChangeTransaction transaction =
        ChangeTransaction.builder()
            .changeEvents(List.of(createEvent))
            .timestamp(auditStamp.getTime())
            .build();
    transactions.add(transaction);

    when(mockTimelineService.getTimeline(
            eq(TEST_DOCUMENT_URN),
            any(Set.class),
            anyLong(),
            anyLong(),
            isNull(),
            isNull(),
            eq(false)))
        .thenReturn(transactions);

    // Execute
    List<DocumentChange> result = resolver.get(mockEnv).get();

    // Verify
    assertNotNull(result);
    assertEquals(result.size(), 1);
    DocumentChange change = result.get(0);
    assertEquals(change.getChangeType(), DocumentChangeType.CREATED);
    assertEquals(change.getDescription(), "Document 'Test Doc' was created");
    assertNotNull(change.getActor());
    assertEquals(change.getActor().getUrn(), TEST_USER_URN.toString());
    assertEquals(change.getTimestamp(), auditStamp.getTime());
  }

  @Test
  public void testGetChangeHistoryWithContentModification() throws Exception {
    List<ChangeTransaction> transactions = new ArrayList<>();

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(TEST_USER_URN);

    // Content modification event
    ChangeEvent contentEvent =
        ChangeEvent.builder()
            .category(ChangeCategory.DOCUMENTATION)
            .operation(ChangeOperation.MODIFY)
            .entityUrn(TEST_DOCUMENT_URN.toString())
            .description("Document title changed from 'Old Title' to 'New Title'")
            .auditStamp(auditStamp)
            .build();

    ChangeTransaction transaction =
        ChangeTransaction.builder().changeEvents(List.of(contentEvent)).build();
    transactions.add(transaction);

    when(mockTimelineService.getTimeline(
            any(Urn.class), any(Set.class), anyLong(), anyLong(), isNull(), isNull(), eq(false)))
        .thenReturn(transactions);

    List<DocumentChange> result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getChangeType(), DocumentChangeType.TITLE_CHANGED);
  }

  @Test
  public void testGetChangeHistoryWithParentChange() throws Exception {
    List<ChangeTransaction> transactions = new ArrayList<>();

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(TEST_USER_URN);

    Map<String, Object> params = new HashMap<>();
    params.put("oldParent", "urn:li:document:old-parent");
    params.put("newParent", "urn:li:document:new-parent");

    ChangeEvent parentEvent =
        ChangeEvent.builder()
            .category(ChangeCategory.PARENT)
            .operation(ChangeOperation.MODIFY)
            .entityUrn(TEST_DOCUMENT_URN.toString())
            .description("Document moved from old parent to new parent")
            .auditStamp(auditStamp)
            .parameters(params)
            .build();

    ChangeTransaction transaction =
        ChangeTransaction.builder()
            .changeEvents(List.of(parentEvent))
            .timestamp(auditStamp.getTime())
            .build();
    transactions.add(transaction);

    when(mockTimelineService.getTimeline(
            any(Urn.class), any(Set.class), anyLong(), anyLong(), isNull(), isNull(), eq(false)))
        .thenReturn(transactions);

    List<DocumentChange> result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.size(), 1);
    DocumentChange change = result.get(0);
    assertEquals(change.getChangeType(), DocumentChangeType.PARENT_CHANGED);
    assertNotNull(change.getDetails());
    assertEquals(change.getDetails().size(), 2);
  }

  @Test
  public void testGetChangeHistoryWithStateChange() throws Exception {
    List<ChangeTransaction> transactions = new ArrayList<>();

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(TEST_USER_URN);

    ChangeEvent stateEvent =
        ChangeEvent.builder()
            .category(ChangeCategory.LIFECYCLE)
            .operation(ChangeOperation.MODIFY)
            .entityUrn(TEST_DOCUMENT_URN.toString())
            .description("Document state changed from UNPUBLISHED to PUBLISHED")
            .auditStamp(auditStamp)
            .build();

    ChangeTransaction transaction =
        ChangeTransaction.builder().changeEvents(List.of(stateEvent)).build();
    transactions.add(transaction);

    when(mockTimelineService.getTimeline(
            any(Urn.class), any(Set.class), anyLong(), anyLong(), isNull(), isNull(), eq(false)))
        .thenReturn(transactions);

    List<DocumentChange> result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getChangeType(), DocumentChangeType.STATE_CHANGED);
  }

  @Test
  public void testGetChangeHistoryWithCustomTimeRange() throws Exception {
    long startTime = System.currentTimeMillis() - 86400000; // 1 day ago
    long endTime = System.currentTimeMillis();

    when(mockEnv.getArgument("startTimeMillis")).thenReturn(startTime);
    when(mockEnv.getArgument("endTimeMillis")).thenReturn(endTime);
    when(mockEnv.getArgument("limit")).thenReturn(100);

    when(mockTimelineService.getTimeline(
            eq(TEST_DOCUMENT_URN),
            any(Set.class),
            eq(startTime),
            eq(endTime),
            isNull(),
            isNull(),
            eq(false)))
        .thenReturn(new ArrayList<>());

    List<DocumentChange> result = resolver.get(mockEnv).get();

    assertNotNull(result);
    verify(mockTimelineService, times(1))
        .getTimeline(
            eq(TEST_DOCUMENT_URN),
            any(Set.class),
            eq(startTime),
            eq(endTime),
            isNull(),
            isNull(),
            eq(false));
  }

  @Test
  public void testGetChangeHistoryMultipleChanges() throws Exception {
    List<ChangeTransaction> transactions = new ArrayList<>();

    AuditStamp auditStamp1 = new AuditStamp();
    auditStamp1.setTime(1000L);
    auditStamp1.setActor(TEST_USER_URN);

    AuditStamp auditStamp2 = new AuditStamp();
    auditStamp2.setTime(2000L);
    auditStamp2.setActor(TEST_USER_URN);

    ChangeEvent event1 =
        ChangeEvent.builder()
            .category(ChangeCategory.LIFECYCLE)
            .operation(ChangeOperation.CREATE)
            .description("Document created")
            .auditStamp(auditStamp1)
            .build();

    ChangeEvent event2 =
        ChangeEvent.builder()
            .category(ChangeCategory.DOCUMENTATION)
            .operation(ChangeOperation.MODIFY)
            .description("Content modified")
            .auditStamp(auditStamp2)
            .build();

    ChangeTransaction transaction1 =
        ChangeTransaction.builder().changeEvents(List.of(event1)).build();
    ChangeTransaction transaction2 =
        ChangeTransaction.builder().changeEvents(List.of(event2)).build();
    transactions.add(transaction1);
    transactions.add(transaction2);

    when(mockTimelineService.getTimeline(
            any(Urn.class), any(Set.class), anyLong(), anyLong(), isNull(), isNull(), eq(false)))
        .thenReturn(transactions);

    List<DocumentChange> result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.size(), 2);
    // Should be sorted by timestamp descending (most recent first)
    assertTrue(result.get(0).getTimestamp() >= result.get(1).getTimestamp());
  }

  @Test(expectedExceptions = Exception.class)
  public void testGetChangeHistoryServiceThrowsException() throws Exception {
    when(mockTimelineService.getTimeline(
            any(Urn.class), any(Set.class), anyLong(), anyLong(), isNull(), isNull(), eq(false)))
        .thenThrow(new RuntimeException("Service error"));

    // Should throw an exception when service fails
    resolver.get(mockEnv).get();
  }

  @Test
  public void testGetChangeHistoryEmptyResult() throws Exception {
    when(mockTimelineService.getTimeline(
            any(Urn.class), any(Set.class), anyLong(), anyLong(), isNull(), isNull(), eq(false)))
        .thenReturn(new ArrayList<>());

    List<DocumentChange> result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetChangeHistoryRespectsLimit() throws Exception {
    // Create more changes than the limit
    List<ChangeTransaction> transactions = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      AuditStamp auditStamp = new AuditStamp();
      auditStamp.setTime(System.currentTimeMillis() + i);
      auditStamp.setActor(TEST_USER_URN);

      ChangeEvent event =
          ChangeEvent.builder()
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.MODIFY)
              .description("Change " + i)
              .auditStamp(auditStamp)
              .build();

      ChangeTransaction transaction =
          ChangeTransaction.builder().changeEvents(List.of(event)).build();
      transactions.add(transaction);
    }

    when(mockEnv.getArgument("limit")).thenReturn(10);
    when(mockTimelineService.getTimeline(
            any(Urn.class), any(Set.class), anyLong(), anyLong(), isNull(), isNull(), eq(false)))
        .thenReturn(transactions);

    List<DocumentChange> result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.size(), 10); // Should respect the limit
  }
}
