package com.linkedin.metadata.systemmetadata.scroll;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.systemmetadata.scroll.PostgresSystemMetadataScrollClient.Cursor;
import com.linkedin.metadata.systemmetadata.scroll.PostgresSystemMetadataScrollClient.SqlPlan;
import io.ebean.Database;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link PostgresSystemMetadataScrollClient}.
 *
 * <p>These tests focus on the SQL plan that the client generates from a {@link
 * SystemMetadataScrollRequest}. They intentionally do not exercise the JDBC path - that is covered
 * by the Postgres integration tests against a live database.
 */
public class PostgresSystemMetadataScrollClientTest {

  private static final String SCHEMA = "datahub";

  private PostgresSystemMetadataScrollClient client;

  @BeforeMethod
  public void setup() {
    PostgresSqlSetupProperties props = mock(PostgresSqlSetupProperties.class);
    when(props.normalizedPostgresSchema()).thenReturn(SCHEMA);
    Database database = mock(Database.class);
    client = new PostgresSystemMetadataScrollClient(database, props);
  }

  @Test
  public void testEntityTypePrefixOnlyAppliesUrnLikeAndOrder() {
    SystemMetadataScrollRequest req =
        SystemMetadataScrollRequest.builder().entityType("assertion").batchSize(50).build();

    SqlPlan plan = client.buildPlan(req);

    assertTrue(
        plan.sql.startsWith("SELECT urn, aspect FROM " + SCHEMA + ".system_metadata_service_v1"),
        "Expected qualified SELECT, got: " + plan.sql);
    assertTrue(plan.sql.contains("urn LIKE ? ESCAPE '\\'"), "Expected URN prefix LIKE clause");
    // Default request has no soft-delete inclusion -> a removed filter is appended.
    assertTrue(plan.sql.contains("NOT COALESCE(removed, false)"));
    assertTrue(plan.sql.endsWith("ORDER BY urn, aspect LIMIT ?"), "Expected keyset ORDER + LIMIT");
    assertEquals(plan.params.get(0), "urn:li:assertion:%");
    // Last bound parameter is the batch size.
    assertEquals(plan.params.get(plan.params.size() - 1), 50);
  }

  @Test
  public void testIncludeSoftDeletedDropsRemovedFilter() {
    SystemMetadataScrollRequest req =
        SystemMetadataScrollRequest.builder()
            .entityType("dataset")
            .includeSoftDeleted(true)
            .batchSize(25)
            .build();

    SqlPlan plan = client.buildPlan(req);

    assertFalse(
        plan.sql.contains("removed"),
        "includeSoftDeleted=true should not generate a removed filter; sql=" + plan.sql);
  }

  @Test
  public void testUrnFilterUsesInClauseWithExactValues() {
    Urn a = UrnUtils.getUrn("urn:li:assertion:a");
    Urn b = UrnUtils.getUrn("urn:li:assertion:b");
    Set<Urn> urns = new LinkedHashSet<>(Arrays.asList(a, b));

    SystemMetadataScrollRequest req =
        SystemMetadataScrollRequest.builder()
            .entityType("assertion")
            .urns(urns)
            .batchSize(10)
            .build();

    SqlPlan plan = client.buildPlan(req);
    assertTrue(plan.sql.contains("urn IN (?,?)"), "Expected IN clause for two URNs: " + plan.sql);
    assertTrue(plan.params.contains(a.toString()));
    assertTrue(plan.params.contains(b.toString()));
  }

  @Test
  public void testAspectFilterUsesInClauseAndOrdering() {
    SystemMetadataScrollRequest req =
        SystemMetadataScrollRequest.builder()
            .entityType("assertion")
            .aspects(List.of("assertionInfo", "status"))
            .batchSize(100)
            .build();

    SqlPlan plan = client.buildPlan(req);
    assertTrue(
        plan.sql.contains("aspect IN (?,?)"),
        "Expected aspect IN-list with two placeholders: " + plan.sql);
    assertTrue(plan.params.contains("assertionInfo"));
    assertTrue(plan.params.contains("status"));
  }

  @Test
  public void testTimestampFiltersGeneratesJsonbBranches() {
    long ge = 1_700_000_000_000L;
    long le = 1_700_999_999_999L;
    SystemMetadataScrollRequest req =
        SystemMetadataScrollRequest.builder()
            .entityType("assertion")
            .gePitEpochMs(ge)
            .lePitEpochMs(le)
            .batchSize(10)
            .build();

    SqlPlan plan = client.buildPlan(req);
    // Must reference both modified-time branch and created-time fallback branch.
    assertTrue(
        plan.sql.contains("(document -> 'aspectModifiedTime') IS NOT NULL"),
        "Expected modifiedTime branch: " + plan.sql);
    assertTrue(
        plan.sql.contains("(document -> 'aspectModifiedTime') IS NULL"),
        "Expected createdTime fallback branch: " + plan.sql);
    assertTrue(plan.sql.contains("(document->>'aspectModifiedTime')::bigint >= ?"));
    assertTrue(plan.sql.contains("(document->>'aspectModifiedTime')::bigint <= ?"));
    assertTrue(plan.sql.contains("(document->>'aspectCreatedTime')::bigint >= ?"));
    assertTrue(plan.sql.contains("(document->>'aspectCreatedTime')::bigint <= ?"));
    // Both branches bind ge and le, so we expect both bounds present in the bound params.
    assertEquals(plan.params.stream().filter(p -> p.equals(ge)).count(), 2L);
    assertEquals(plan.params.stream().filter(p -> p.equals(le)).count(), 2L);
  }

  /**
   * Regression test for the bug where the JSONB key-existence operator {@code ?} was interpreted as
   * a JDBC PreparedStatement bind placeholder, producing a {@code "No value specified for parameter
   * N"} error at execute time. The number of {@code ?} characters in the SQL must equal the number
   * of bound params. See {@link PostgresSystemMetadataScrollClient#buildPlan}.
   */
  @Test
  public void testPlaceholderCountMatchesParamCountWithJsonbBranches() {
    long ge = 1_700_000_000_000L;
    long le = 1_700_999_999_999L;
    SystemMetadataScrollRequest req =
        SystemMetadataScrollRequest.builder()
            .entityType("assertion")
            .urns(
                new LinkedHashSet<>(
                    Arrays.asList(
                        UrnUtils.getUrn("urn:li:assertion:a"),
                        UrnUtils.getUrn("urn:li:assertion:b"))))
            .aspects(List.of("assertionInfo", "status"))
            .gePitEpochMs(ge)
            .lePitEpochMs(le)
            .scrollId(
                PostgresSystemMetadataScrollClient.encodeCursor("urn:li:assertion:abc", "info"))
            .batchSize(10)
            .build();

    SqlPlan plan = client.buildPlan(req);

    long placeholderCount = plan.sql.chars().filter(c -> c == '?').count();
    assertEquals(
        placeholderCount,
        plan.params.size(),
        "Mismatch between '?' bind placeholders and bound params: sql="
            + plan.sql
            + ", params="
            + plan.params);

    // Defensive: the JSONB key-existence operator must not appear in the generated SQL because
    // pgJDBC mistakes it for a bind placeholder.
    assertFalse(
        plan.sql.contains("document ? '"),
        "JSONB ? operator must not be used in PreparedStatement SQL; sql=" + plan.sql);
  }

  @Test
  public void testScrollIdAppliesKeysetTupleComparison() {
    String cursor = PostgresSystemMetadataScrollClient.encodeCursor("urn:li:assertion:abc", "info");
    SystemMetadataScrollRequest req =
        SystemMetadataScrollRequest.builder()
            .entityType("assertion")
            .scrollId(cursor)
            .batchSize(10)
            .build();

    SqlPlan plan = client.buildPlan(req);
    assertTrue(
        plan.sql.contains("(urn, aspect) > (?, ?)"),
        "Expected keyset tuple comparison: " + plan.sql);
    assertTrue(plan.params.contains("urn:li:assertion:abc"));
    assertTrue(plan.params.contains("info"));
  }

  @Test
  public void testCursorRoundTrip() {
    String urn = "urn:li:dataset:(urn:li:dataPlatform:test,t,PROD)";
    String aspect = "schemaMetadata";
    String token = PostgresSystemMetadataScrollClient.encodeCursor(urn, aspect);
    Cursor decoded = PostgresSystemMetadataScrollClient.decodeCursor(token);
    assertNotNull(decoded);
    assertEquals(decoded.urn, urn);
    assertEquals(decoded.aspect, aspect);
  }

  @Test
  public void testCursorTolerateNullAndBlank() {
    assertNull(PostgresSystemMetadataScrollClient.decodeCursor(null));
    assertNull(PostgresSystemMetadataScrollClient.decodeCursor(""));
    assertNull(PostgresSystemMetadataScrollClient.decodeCursor("   "));
  }

  @Test
  public void testCursorIgnoresMalformedToken() {
    // A garbage non-base64 string should be treated as "start from the beginning".
    assertNull(PostgresSystemMetadataScrollClient.decodeCursor("not-a-base64-cursor!"));
  }
}
