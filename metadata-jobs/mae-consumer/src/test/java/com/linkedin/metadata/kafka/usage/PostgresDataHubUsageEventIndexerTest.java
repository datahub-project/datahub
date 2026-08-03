package com.linkedin.metadata.kafka.usage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventInsertRow;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsStore;
import com.linkedin.metadata.kafka.transformer.DataHubUsageEventTransformer;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies that {@link PostgresDataHubUsageEventIndexer} commits a Kafka batch in a single store
 * insert (concern 7), skips JSON-invalid records without poisoning the rest of the batch, and
 * propagates SQL failures so the Kafka listener can back off.
 */
public class PostgresDataHubUsageEventIndexerTest {

  private PostgresUsageEventsStore store;
  private OperationContext systemOperationContext;
  private PostgresDataHubUsageEventIndexer indexer;

  @BeforeMethod
  public void setUp() {
    store = mock(PostgresUsageEventsStore.class);
    systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    indexer = new PostgresDataHubUsageEventIndexer(store, systemOperationContext);
  }

  @Test
  public void testIndexBatch_emptyInputIsNoOp() throws Exception {
    indexer.indexBatch(systemOperationContext, Collections.emptyList());
    verify(store, times(0)).insertBatch(any());
  }

  @Test
  public void testIndexBatch_singleStoreInsertForEntireBatch() throws Exception {
    // Three valid events ⇒ exactly one insertBatch call carrying all three rows.
    List<DataHubUsageEventIndexer.IndexableUsageEvent> events =
        List.of(
            event("id-1", 1_700_000_000_000L, "EntityViewEvent", "urn:li:corpuser:alice"),
            event("id-2", 1_700_000_001_000L, "SearchEvent", "urn:li:corpuser:bob"),
            event("id-3", 1_700_000_002_000L, "EntityActionEvent", "urn:li:corpuser:carol"));

    indexer.indexBatch(systemOperationContext, events);

    verify(store, times(1))
        .insertBatch(
            argThat(
                (List<PostgresUsageEventInsertRow> rows) ->
                    rows.size() == 3
                        && rows.get(0).getId().equals("id-1")
                        && rows.get(1).getId().equals("id-2")
                        && rows.get(2).getId().equals("id-3")));
  }

  @Test
  public void testIndexBatch_skipsBadJsonAndContinues() throws Exception {
    // The first event has invalid JSON; the remaining two must still flow through to the store.
    List<DataHubUsageEventIndexer.IndexableUsageEvent> events =
        List.of(
            new DataHubUsageEventIndexer.IndexableUsageEvent(
                new DataHubUsageEventTransformer.TransformedDocument("bad-id", "{not json}"),
                "bad-id_00001"),
            event("id-good-1", 1_700_000_000_000L, "EntityViewEvent", "urn:li:corpuser:alice"),
            event("id-good-2", 1_700_000_001_000L, "SearchEvent", "urn:li:corpuser:bob"));

    indexer.indexBatch(systemOperationContext, events);

    verify(store, times(1))
        .insertBatch(argThat((List<PostgresUsageEventInsertRow> rows) -> rows.size() == 2));
  }

  @Test
  public void testIndexBatch_allBadJsonSkipsStoreCallEntirely() throws Exception {
    List<DataHubUsageEventIndexer.IndexableUsageEvent> events =
        List.of(
            new DataHubUsageEventIndexer.IndexableUsageEvent(
                new DataHubUsageEventTransformer.TransformedDocument("bad", "{nope}"), "bad_0"));

    indexer.indexBatch(systemOperationContext, events);

    verify(store, times(0)).insertBatch(any());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testIndexBatch_propagatesStoreSqlFailure() throws Exception {
    doThrow(new SQLException("boom")).when(store).insertBatch(any(java.util.List.class));

    List<DataHubUsageEventIndexer.IndexableUsageEvent> events =
        List.of(event("id-1", 1_700_000_000_000L, "EntityViewEvent", "urn:li:corpuser:alice"));

    try {
      indexer.indexBatch(systemOperationContext, events);
      Assert.fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      // Make sure the cause is preserved so the Kafka error handler can react.
      Assert.assertTrue(e.getCause() instanceof SQLException);
      throw e;
    }
  }

  private static DataHubUsageEventIndexer.IndexableUsageEvent event(
      String id, long timestampMs, String eventType, String actorUrn) {
    String json =
        String.format(
            "{\"type\":\"%s\",\"timestamp\":%d,\"actorUrn\":\"%s\"}",
            eventType, timestampMs, actorUrn);
    return new DataHubUsageEventIndexer.IndexableUsageEvent(
        new DataHubUsageEventTransformer.TransformedDocument(id, json), id);
  }
}
