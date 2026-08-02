package com.linkedin.metadata.timeseries.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectService.SortKey;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectService.SortValueKind;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class PostgresTimeseriesScrollKeysetTest {

  @Test
  public void resolveSortKeys_empty_defaultsToEventTimeMessageIdDesc() {
    List<SortKey> keys = PostgresTimeseriesAspectService.resolveSortKeys(Collections.emptyList());
    assertEquals(keys.size(), 2);
    assertEquals(keys.get(0).sqlExpr(), "event_time");
    assertFalse(keys.get(0).ascending());
    assertEquals(keys.get(0).valueKind(), SortValueKind.EVENT_TIME);
    assertEquals(keys.get(1).sqlExpr(), "message_id");
    assertFalse(keys.get(1).ascending());
    assertEquals(keys.get(1).valueKind(), SortValueKind.MESSAGE_ID);
  }

  @Test
  public void resolveSortKeys_appendsMessageIdTiebreakerMatchingLastDirection() {
    List<SortKey> keys =
        PostgresTimeseriesAspectService.resolveSortKeys(
            List.of(
                new SortCriterion()
                    .setField(MappingsBuilder.TIMESTAMP_MILLIS_FIELD)
                    .setOrder(SortOrder.ASCENDING)));
    assertEquals(keys.size(), 2);
    assertTrue(keys.get(0).ascending());
    assertEquals(keys.get(1).valueKind(), SortValueKind.MESSAGE_ID);
    assertTrue(keys.get(1).ascending());
  }

  @Test
  public void resolveSortKeys_doesNotDuplicateMessageId() {
    List<SortKey> keys =
        PostgresTimeseriesAspectService.resolveSortKeys(
            List.of(
                new SortCriterion()
                    .setField(MappingsBuilder.TIMESTAMP_MILLIS_FIELD)
                    .setOrder(SortOrder.DESCENDING),
                new SortCriterion()
                    .setField(MappingsBuilder.MESSAGE_ID_FIELD)
                    .setOrder(SortOrder.DESCENDING)));
    assertEquals(keys.size(), 2);
    assertEquals(keys.get(1).valueKind(), SortValueKind.MESSAGE_ID);
  }

  @Test
  public void appendKeysetPredicate_mixedDirections_expandsOrChain() {
    List<SortKey> keys =
        List.of(
            new SortKey("event_time", false, SortValueKind.EVENT_TIME),
            new SortKey("message_id", true, SortValueKind.MESSAGE_ID));
    StringBuilder where = new StringBuilder("WHERE 1=1");
    List<Object> params = new ArrayList<>();
    PostgresTimeseriesAspectService.appendKeysetPredicate(
        where, params, keys, List.of(1000L, "msg-a"));

    String sql = where.toString();
    assertTrue(sql.contains("event_time < ?"));
    assertTrue(sql.contains("event_time IS NOT DISTINCT FROM ? AND message_id > ?"));
    assertEquals(params.size(), 3);
    assertTrue(params.get(0) instanceof java.sql.Timestamp);
    assertEquals(params.get(1), new java.sql.Timestamp(1000L));
    assertEquals(params.get(2), "msg-a");
  }
}
