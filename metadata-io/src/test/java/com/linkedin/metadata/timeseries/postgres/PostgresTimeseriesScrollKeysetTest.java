package com.linkedin.metadata.timeseries.postgres;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectService.SortKey;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectService.SortValueKind;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  public void appendKeysetPredicate_nullCursorValue_skipsKeyArm() {
    List<SortKey> keys =
        List.of(
            new SortKey("event_time", false, SortValueKind.EVENT_TIME),
            new SortKey("message_id", true, SortValueKind.MESSAGE_ID));
    StringBuilder where = new StringBuilder("WHERE 1=1");
    List<Object> params = new ArrayList<>();
    List<Object> cursor = new ArrayList<>();
    cursor.add(1000L);
    cursor.add(null);
    PostgresTimeseriesAspectService.appendKeysetPredicate(where, params, keys, cursor);

    String sql = where.toString();
    assertFalse(sql.contains("message_id IS NULL"));
    assertTrue(sql.contains("event_time < ? OR event_time IS NULL"));
    assertEquals(params.size(), 1);
  }

  @Test
  public void appendKeysetPredicate_allNullCursorValues_matchesNothing() {
    List<SortKey> keys =
        List.of(
            new SortKey("event_time", false, SortValueKind.EVENT_TIME),
            new SortKey("message_id", true, SortValueKind.MESSAGE_ID));
    StringBuilder where = new StringBuilder("WHERE 1=1");
    List<Object> params = new ArrayList<>();
    List<Object> cursor = new ArrayList<>();
    cursor.add(null);
    cursor.add(null);
    PostgresTimeseriesAspectService.appendKeysetPredicate(where, params, keys, cursor);

    assertTrue(where.toString().contains("1=0"));
    assertTrue(params.isEmpty());
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
    assertTrue(sql.contains("event_time < ? OR event_time IS NULL"));
    assertTrue(
        sql.contains(
            "event_time IS NOT DISTINCT FROM ? AND (message_id > ? OR message_id IS NULL)"));
    assertEquals(params.size(), 3);
    assertTrue(params.get(0) instanceof java.sql.Timestamp);
    assertEquals(params.get(1), new java.sql.Timestamp(1000L));
    assertEquals(params.get(2), "msg-a");
  }

  @Test
  public void fromCriterion_mapsJdbcKindsFromSearchableFieldTypes() {
    SortKey longKey =
        SortKey.fromCriterion(
            new SortCriterion().setField("rowCount").setOrder(SortOrder.DESCENDING),
            Map.of("rowCount", Set.of(FieldType.COUNT)),
            null);
    assertEquals(longKey.valueKind(), SortValueKind.DOCUMENT_LONG);
    assertTrue(longKey.sqlExpr().contains("::bigint"));

    SortKey doubleKey =
        SortKey.fromCriterion(
            new SortCriterion().setField("value").setOrder(SortOrder.ASCENDING),
            Map.of("value", Set.of(FieldType.DOUBLE)),
            null);
    assertEquals(doubleKey.valueKind(), SortValueKind.DOCUMENT_DOUBLE);
    assertTrue(doubleKey.sqlExpr().contains("::double precision"));

    SortKey boolKey =
        SortKey.fromCriterion(
            new SortCriterion().setField("isLatest").setOrder(SortOrder.ASCENDING),
            Map.of("isLatest", Set.of(FieldType.BOOLEAN)),
            null);
    assertEquals(boolKey.valueKind(), SortValueKind.DOCUMENT_BOOLEAN);
    assertTrue(boolKey.sqlExpr().contains("::boolean"));
  }

  @Test
  public void appendKeysetPredicate_bindsTypedDocumentValuesNotVarchar() {
    List<SortKey> keys =
        List.of(
            new SortKey(
                "(document #>> ARRAY['rowCount'])::bigint", false, SortValueKind.DOCUMENT_LONG),
            new SortKey(
                "(document #>> ARRAY['value'])::double precision",
                false,
                SortValueKind.DOCUMENT_DOUBLE),
            new SortKey(
                "(document #>> ARRAY['isLatest'])::boolean",
                false,
                SortValueKind.DOCUMENT_BOOLEAN));
    StringBuilder where = new StringBuilder("WHERE 1=1");
    List<Object> params = new ArrayList<>();
    PostgresTimeseriesAspectService.appendKeysetPredicate(
        where, params, keys, List.of(42L, 1.5d, true));

    assertEquals(params.size(), 6);
    assertEquals(params.get(0), 42L);
    assertEquals(params.get(1), 42L);
    assertEquals(params.get(2), 1.5d);
    assertEquals(params.get(3), 42L);
    assertEquals(params.get(4), 1.5d);
    assertTrue(params.get(5) instanceof Boolean);
    assertEquals(params.get(5), true);
    for (Object param : params) {
      assertFalse(param instanceof String, "typed cursor values must not bind as varchar");
    }
  }

  @Test
  public void scrollCursor_roundTripsNumericBooleanAndText() {
    List<SortKey> keys =
        List.of(
            new SortKey("c0", false, SortValueKind.DOCUMENT_LONG),
            new SortKey("c1", false, SortValueKind.DOCUMENT_DOUBLE),
            new SortKey("c2", false, SortValueKind.DOCUMENT_BOOLEAN),
            new SortKey("c3", false, SortValueKind.DOCUMENT_TEXT));
    List<Object> original = List.of(42L, 1.5d, true, "abc");
    String encoded = PostgresTimeseriesAspectService.encodeScrollCursor(original);
    List<Object> decoded = PostgresTimeseriesAspectService.decodeScrollCursor(encoded, keys);
    assertEquals(decoded.size(), 4);
    assertEquals(decoded.get(0), 42L);
    assertEquals(decoded.get(1), 1.5d);
    assertEquals(decoded.get(2), true);
    assertEquals(decoded.get(3), "abc");
  }

  @Test
  public void readSortValues_preservesJdbcTypes() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getLong("_sk0")).thenReturn(42L);
    when(rs.getDouble("_sk1")).thenReturn(1.5d);
    when(rs.getBoolean("_sk2")).thenReturn(true);
    when(rs.getString("_sk3")).thenReturn("abc");
    when(rs.wasNull()).thenReturn(false);

    List<SortKey> keys =
        List.of(
            new SortKey("c0", false, SortValueKind.DOCUMENT_LONG),
            new SortKey("c1", false, SortValueKind.DOCUMENT_DOUBLE),
            new SortKey("c2", false, SortValueKind.DOCUMENT_BOOLEAN),
            new SortKey("c3", false, SortValueKind.DOCUMENT_TEXT));
    List<Object> values = PostgresTimeseriesAspectService.readSortValues(rs, keys);
    assertEquals(values.get(0), 42L);
    assertEquals(values.get(1), 1.5d);
    assertEquals(values.get(2), true);
    assertEquals(values.get(3), "abc");
  }

  @Test
  public void decodeScrollCursor_malformed_throws() {
    List<SortKey> keys = PostgresTimeseriesAspectService.resolveSortKeys(Collections.emptyList());
    expectThrows(
        IllegalArgumentException.class,
        () -> PostgresTimeseriesAspectService.decodeScrollCursor("not-a-cursor", keys));
  }
}
