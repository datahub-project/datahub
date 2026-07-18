package com.linkedin.metadata.systemmetadata.scroll;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.systemmetadata.PostgresSystemMetadataService;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * PostgreSQL-backed implementation of {@link SystemMetadataScrollClient}.
 *
 * <p>Reads from the {@link PostgresSystemMetadataService#TABLE_NAME} table using keyset pagination
 * on {@code (urn, aspect)} so the consistency framework can scan large catalogs without OFFSET
 * cost.
 *
 * <p>Continuation tokens are an opaque base64-encoded JSON document of the last seen {@code (urn,
 * aspect)} pair. They are not portable across implementations (an Elasticsearch scrollId cannot be
 * passed in here, and vice versa) but the higher-level scrollId in {@code CheckResult} is opaque to
 * API callers either way, so this is invisible above the client boundary.
 */
@Slf4j
public class PostgresSystemMetadataScrollClient implements SystemMetadataScrollClient {

  /** Continuation-token shape: {@code {"u":"<urn>","a":"<aspect>"}}, base64-url encoded. */
  private static final String CURSOR_KEY_URN = "u";

  private static final String CURSOR_KEY_ASPECT = "a";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Database database;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;

  public PostgresSystemMetadataScrollClient(
      @Nonnull Database database, @Nonnull PostgresSqlSetupProperties postgresSqlSetupProperties) {
    this.database = database;
    this.postgresSqlSetupProperties = postgresSqlSetupProperties;
  }

  private String qualifiedTable() {
    return postgresSqlSetupProperties.normalizedPostgresSchema()
        + "."
        + PostgresSystemMetadataService.TABLE_NAME;
  }

  @Override
  @Nonnull
  public SystemMetadataScrollResult scrollUrns(
      @Nonnull OperationContext opContext, @Nonnull SystemMetadataScrollRequest request) {
    SqlPlan plan = buildPlan(request);

    // Stream rows in (urn, aspect) order so we can both deduplicate URNs in this page and
    // produce a deterministic keyset cursor from the last row read.
    Set<Urn> urns = new LinkedHashSet<>();
    String lastUrn = null;
    String lastAspect = null;
    int rowsRead = 0;

    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(plan.sql)) {
      for (int i = 0; i < plan.params.size(); i++) {
        ps.setObject(i + 1, plan.params.get(i));
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          rowsRead++;
          String urnStr = rs.getString(1);
          String aspect = rs.getString(2);
          lastUrn = urnStr;
          lastAspect = aspect;
          try {
            urns.add(UrnUtils.getUrn(urnStr));
          } catch (Exception e) {
            log.warn(
                "Skipping invalid URN {} from system_metadata scroll: {}", urnStr, e.getMessage());
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          "Failed to scroll system metadata for entityType=" + request.getEntityType(), e);
    }

    // Only return a continuation token when the page was full; if we read fewer rows than the
    // batch size we know there is no more data.
    String nextScrollId =
        rowsRead >= request.getBatchSize() && lastUrn != null
            ? encodeCursor(lastUrn, lastAspect)
            : null;

    return SystemMetadataScrollResult.builder().urns(urns).nextScrollId(nextScrollId).build();
  }

  /**
   * SQL builder result: the parameterized statement plus its bound values. Package-private so unit
   * tests can inspect the generated SQL.
   */
  static final class SqlPlan {
    final String sql;
    final List<Object> params;

    SqlPlan(String sql, List<Object> params) {
      this.sql = sql;
      this.params = params;
    }
  }

  @Nonnull
  SqlPlan buildPlan(@Nonnull SystemMetadataScrollRequest request) {
    StringBuilder sql =
        new StringBuilder("SELECT urn, aspect FROM ").append(qualifiedTable()).append(" WHERE ");
    List<String> clauses = new ArrayList<>();
    List<Object> params = new ArrayList<>();

    // Entity-type prefix. Use LIKE with literal escape so percent/underscore in entity-type
    // names (none today, but defensive) don't widen the scan.
    clauses.add("urn LIKE ? ESCAPE '\\'");
    params.add("urn:li:" + escapeLikeLiteral(request.getEntityType()) + ":%");

    Set<Urn> urns = request.getUrns();
    if (urns != null && !urns.isEmpty()) {
      clauses.add("urn IN (" + placeholders(urns.size()) + ")");
      for (Urn u : urns) {
        params.add(u.toString());
      }
    }

    List<String> aspects = request.getAspects();
    if (aspects != null && !aspects.isEmpty()) {
      clauses.add("aspect IN (" + placeholders(aspects.size()) + ")");
      params.addAll(aspects);
    }

    Long ge = request.getGePitEpochMs();
    Long le = request.getLePitEpochMs();
    if (ge != null || le != null) {
      // Mirror the ES query semantics: prefer aspectModifiedTime, fall back to aspectCreatedTime
      // when modified is absent on the document. Both are stored in the JSONB document column.
      //
      // We deliberately avoid the JSONB key-existence operator `document ? 'key'` here because the
      // PostgreSQL JDBC driver scans PreparedStatement SQL for `?` placeholders and would
      // (incorrectly) count this as a bind parameter, producing
      // `org.postgresql.util.PSQLException: No value specified for parameter N` at execute time.
      // `(document -> 'key') IS NOT NULL` is functionally equivalent for these timestamp fields
      // (they are stored as JSON numbers, never JSON null) and uses only the `->` operator which
      // the driver does not confuse with placeholders.
      StringBuilder ts = new StringBuilder("(");
      ts.append("((document -> 'aspectModifiedTime') IS NOT NULL");
      if (ge != null) {
        ts.append(" AND (document->>'aspectModifiedTime')::bigint >= ?");
        params.add(ge);
      }
      if (le != null) {
        ts.append(" AND (document->>'aspectModifiedTime')::bigint <= ?");
        params.add(le);
      }
      ts.append(")");
      ts.append(" OR ((document -> 'aspectModifiedTime') IS NULL");
      if (ge != null) {
        ts.append(" AND (document->>'aspectCreatedTime')::bigint >= ?");
        params.add(ge);
      }
      if (le != null) {
        ts.append(" AND (document->>'aspectCreatedTime')::bigint <= ?");
        params.add(le);
      }
      ts.append("))");
      clauses.add(ts.toString());
    }

    if (!request.isIncludeSoftDeleted()) {
      clauses.add("NOT COALESCE(removed, false)");
    }

    Cursor cursor = decodeCursor(request.getScrollId());
    if (cursor != null) {
      // Row-tuple comparison gives keyset semantics matching ORDER BY urn, aspect.
      clauses.add("(urn, aspect) > (?, ?)");
      params.add(cursor.urn);
      params.add(cursor.aspect);
    }

    sql.append(String.join(" AND ", clauses));
    sql.append(" ORDER BY urn, aspect LIMIT ?");
    params.add(request.getBatchSize());

    return new SqlPlan(sql.toString(), params);
  }

  // ============================================================================
  // Cursor encoding
  // ============================================================================

  static final class Cursor {
    final String urn;
    final String aspect;

    Cursor(String urn, String aspect) {
      this.urn = urn;
      this.aspect = aspect;
    }
  }

  static String encodeCursor(@Nonnull String urn, @Nullable String aspect) {
    try {
      byte[] bytes =
          MAPPER.writeValueAsBytes(
              Map.of(CURSOR_KEY_URN, urn, CURSOR_KEY_ASPECT, aspect == null ? "" : aspect));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to encode scroll cursor", e);
    }
  }

  @Nullable
  static Cursor decodeCursor(@Nullable String scrollId) {
    if (scrollId == null || scrollId.isBlank()) {
      return null;
    }
    try {
      byte[] raw = Base64.getUrlDecoder().decode(scrollId);
      JsonNode n = MAPPER.readTree(raw);
      if (!n.has(CURSOR_KEY_URN)) {
        return null;
      }
      String urn = n.get(CURSOR_KEY_URN).asText();
      String aspect = n.has(CURSOR_KEY_ASPECT) ? n.get(CURSOR_KEY_ASPECT).asText() : "";
      return new Cursor(urn, aspect);
    } catch (Exception e) {
      // Tolerate stale or malformed tokens by restarting the scan from the beginning rather
      // than failing the whole consistency batch.
      log.warn(
          "Ignoring unparseable Postgres scroll cursor (length={}): {}",
          scrollId.length(),
          e.getMessage());
      return null;
    }
  }

  // ============================================================================
  // Misc
  // ============================================================================

  private static String placeholders(int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append('?');
    }
    return sb.toString();
  }

  private static String escapeLikeLiteral(@Nonnull String s) {
    // Backslash-escape the LIKE wildcards so a future entity-type containing % or _ stays literal.
    StringBuilder out = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (ch == '%' || ch == '_' || ch == '\\') {
        out.append('\\');
      }
      out.append(ch);
    }
    // Validate UTF-8 round-trip eagerly to surface bad input early.
    new String(out.toString().getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
    return out.toString();
  }
}
