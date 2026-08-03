package com.linkedin.metadata.systemmetadata;

import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.FIELD_ASPECT;
import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.FIELD_LAST_UPDATED;
import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.FIELD_URN;
import static io.datahubproject.metadata.context.SystemTelemetryContext.TELEMETRY_TRACE_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.SystemMetadataServiceConfig;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.tasks.GetTaskResponse;

/**
 * PostgreSQL-backed system metadata when {@code elasticsearch.enabled=false}. Schema and semantics
 * mirror the former OpenSearch index {@code system_metadata_service_v1}.
 */
@Slf4j
public class PostgresSystemMetadataService implements SystemMetadataService {

  public static final String TABLE_NAME = "system_metadata_service_v1";
  private static final String RUN_ID_JSON = "runId";
  private static final String REGISTRY_NAME_JSON = "registryName";
  private static final String REGISTRY_VERSION_JSON = "registryVersion";
  private static final String DOC_DELIMITER = "--";

  private final Database database;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;
  private final SystemMetadataServiceConfig systemMetadataServiceConfig;
  private final String elasticIdHashAlgo;

  public PostgresSystemMetadataService(
      @Nonnull Database database,
      @Nonnull PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Nonnull SystemMetadataServiceConfig systemMetadataServiceConfig,
      @Nonnull String elasticIdHashAlgo) {
    this.database = database;
    this.postgresSqlSetupProperties = postgresSqlSetupProperties;
    this.systemMetadataServiceConfig = systemMetadataServiceConfig;
    this.elasticIdHashAlgo = elasticIdHashAlgo;
  }

  @Override
  public SystemMetadataServiceConfig getSystemMetadataServiceConfig() {
    return systemMetadataServiceConfig;
  }

  private String qualifiedTable() {
    return postgresSqlSetupProperties.normalizedPostgresSchema()
        + "."
        + postgresSqlSetupProperties.normalizedPgSystemMetadataTableName();
  }

  /**
   * Same JSON mapper as {@link OperationContext#getObjectMapper()}: explicit context when present,
   * otherwise {@link ObjectMapperContext#DEFAULT}.
   */
  @Nonnull
  private static ObjectMapper objectMapper(@Nullable OperationContext opContext) {
    return opContext != null
        ? opContext.getObjectMapper()
        : ObjectMapperContext.DEFAULT.getObjectMapper();
  }

  private String toDocId(@Nonnull final String urnString, @Nonnull final String aspect) {
    String rawDocId = urnString + DOC_DELIMITER + aspect;
    try {
      byte[] bytesOfRawDocID = rawDocId.getBytes(StandardCharsets.UTF_8);
      MessageDigest md = MessageDigest.getInstance(elasticIdHashAlgo);
      byte[] digest = md.digest(bytesOfRawDocID);
      return Base64.getEncoder().encodeToString(digest);
    } catch (NoSuchAlgorithmException e) {
      return rawDocId;
    }
  }

  private String toDocument(@Nonnull SystemMetadata systemMetadata, String urn, String aspect) {
    final ObjectNode document = JsonNodeFactory.instance.objectNode();

    document.put(FIELD_URN, urn);
    document.put(FIELD_ASPECT, aspect);
    document.put(RUN_ID_JSON, systemMetadata.getRunId());
    document.put(FIELD_LAST_UPDATED, systemMetadata.getLastObserved());
    document.put(REGISTRY_NAME_JSON, systemMetadata.getRegistryName());
    document.put(REGISTRY_VERSION_JSON, systemMetadata.getRegistryVersion());
    document.put("removed", false);
    if (systemMetadata.getAspectCreated() != null) {
      document.put("aspectCreatedTime", systemMetadata.getAspectCreated().getTime());
      document.put("aspectCreatedActor", systemMetadata.getAspectCreated().getActor().toString());
    }
    if (systemMetadata.getAspectModified() != null) {
      document.put("aspectModifiedTime", systemMetadata.getAspectModified().getTime());
      document.put("aspectModifiedActor", systemMetadata.getAspectModified().getActor().toString());
    }
    if (systemMetadata.getProperties() != null
        && systemMetadata.getProperties().containsKey(TELEMETRY_TRACE_KEY)) {
      document.put(TELEMETRY_TRACE_KEY, systemMetadata.getProperties().get(TELEMETRY_TRACE_KEY));
    }
    return document.toString();
  }

  private static String columnForSqlParamKey(String paramKey) {
    return switch (paramKey) {
      case ElasticSearchSystemMetadataService.FIELD_URN -> "urn";
      case ElasticSearchSystemMetadataService.FIELD_ASPECT -> "aspect";
      case "runId" -> "run_id";
      case "registryName" -> "registry_name";
      case "registryVersion" -> "registry_version";
      default -> null;
    };
  }

  private static AspectRowSummary rowToSummary(JsonNode document) {
    AspectRowSummary summary = new AspectRowSummary();
    summary.setRunId(textOrNull(document.path(RUN_ID_JSON)));
    summary.setAspectName(textOrNull(document.path(FIELD_ASPECT)));
    summary.setUrn(textOrNull(document.path(FIELD_URN)));
    JsonNode tsNode = document.path(FIELD_LAST_UPDATED);
    if (tsNode.isNumber()) {
      summary.setTimestamp(tsNode.longValue());
    }
    String aspectName = summary.getAspectName();
    summary.setKeyAspect(aspectName != null && aspectName.endsWith("Key"));
    if (document.hasNonNull(TELEMETRY_TRACE_KEY)) {
      summary.setTelemetryTraceId(document.get(TELEMETRY_TRACE_KEY).asText(), SetMode.IGNORE_NULL);
    }
    return summary;
  }

  @Nullable
  private static String textOrNull(JsonNode node) {
    if (node == null || node.isMissingNode() || node.isNull()) {
      return null;
    }
    return node.asText();
  }

  @Override
  public void deleteAspect(@Nonnull OperationContext opContext, String urn, String aspect) {
    String sql = "DELETE FROM " + qualifiedTable() + " WHERE urn = ? AND aspect = ?";
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, urn);
      ps.setString(2, aspect);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<GetTaskResponse> getTaskStatus(
      @Nonnull OperationContext opContext, @Nonnull String nodeId, long taskId) {
    return Optional.empty();
  }

  @Override
  public void deleteUrn(@Nonnull OperationContext opContext, String finalOldUrn) {
    String sql = "DELETE FROM " + qualifiedTable() + " WHERE urn = ?";
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, finalOldUrn);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setDocStatus(@Nonnull OperationContext opContext, String urn, boolean removed) {
    List<AspectRowSummary> aspectList =
        findByParams(
            opContext,
            Map.of(FIELD_URN, urn),
            !removed,
            0,
            systemMetadataServiceConfig.getLimit().getResults().getApiDefault());
    for (AspectRowSummary aspect : aspectList) {
      upsertRemovedOnly(toDocId(aspect.getUrn(), aspect.getAspectName()), removed);
    }
  }

  private void upsertRemovedOnly(String docId, boolean removed) {
    String sqlSelect = "SELECT document::text FROM " + qualifiedTable() + " WHERE doc_id = ?";
    String sqlUpsert =
        "INSERT INTO "
            + qualifiedTable()
            + " (doc_id, urn, aspect, run_id, registry_name, registry_version, "
            + "last_updated, removed, document) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)\n"
            + "ON CONFLICT (doc_id) DO UPDATE SET "
            + "removed = EXCLUDED.removed, "
            + "document = EXCLUDED.document";

    try (Connection c = database.dataSource().getConnection()) {
      try (PreparedStatement sel = c.prepareStatement(sqlSelect)) {
        sel.setString(1, docId);
        try (ResultSet rs = sel.executeQuery()) {
          if (!rs.next()) {
            return;
          }
          String docStr = rs.getString(1);
          ObjectNode doc = (ObjectNode) objectMapper(null).readTree(docStr);
          doc.put("removed", removed);

          String urn = doc.path(FIELD_URN).asText();
          String aspect = doc.path(FIELD_ASPECT).asText();
          try (PreparedStatement up = c.prepareStatement(sqlUpsert)) {
            bindRow(up, docId, urn, aspect, doc, removed);
            up.executeUpdate();
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void bindRow(
      PreparedStatement ps,
      String docId,
      String urn,
      String aspect,
      JsonNode document,
      boolean removed)
      throws SQLException {
    ps.setString(1, docId);
    ps.setString(2, urn);
    ps.setString(3, aspect);
    ps.setString(4, textOrNull(document.path(RUN_ID_JSON)));
    ps.setString(5, textOrNull(document.path(REGISTRY_NAME_JSON)));
    ps.setString(6, textOrNull(document.path(REGISTRY_VERSION_JSON)));
    JsonNode tsNode = document.path(FIELD_LAST_UPDATED);
    if (tsNode.isNumber()) {
      ps.setLong(7, tsNode.longValue());
    } else {
      ps.setNull(7, Types.BIGINT);
    }
    ps.setBoolean(8, removed);
    ps.setString(9, document.toString());
  }

  @Override
  public void insert(
      @Nonnull OperationContext opContext,
      @Nullable SystemMetadata systemMetadata,
      String urn,
      String aspect) {
    if (systemMetadata == null) {
      return;
    }
    String docId = toDocId(urn, aspect);
    String docStr = toDocument(systemMetadata, urn, aspect);
    try {
      JsonNode doc = objectMapper(null).readTree(docStr);
      try (Connection c = database.dataSource().getConnection();
          PreparedStatement ps =
              c.prepareStatement(
                  "INSERT INTO "
                      + qualifiedTable()
                      + " (doc_id, urn, aspect, run_id, registry_name, registry_version, "
                      + "last_updated, removed, document) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)\n"
                      + "ON CONFLICT (doc_id) DO UPDATE SET "
                      + "urn = EXCLUDED.urn, aspect = EXCLUDED.aspect, "
                      + "run_id = EXCLUDED.run_id, registry_name = EXCLUDED.registry_name, "
                      + "registry_version = EXCLUDED.registry_version, "
                      + "last_updated = EXCLUDED.last_updated, removed = EXCLUDED.removed, "
                      + "document = EXCLUDED.document")) {
        bindRow(ps, docId, urn, aspect, doc, false);
        ps.executeUpdate();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<AspectRowSummary> findByRunId(
      @Nonnull OperationContext opContext,
      String runId,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size) {
    return findByParams(
        opContext, Collections.singletonMap(RUN_ID_JSON, runId), includeSoftDeleted, from, size);
  }

  @Override
  public List<AspectRowSummary> findByUrn(
      @Nonnull OperationContext opContext,
      String urn,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size) {
    return findByParams(
        opContext, Collections.singletonMap(FIELD_URN, urn), includeSoftDeleted, from, size);
  }

  @Override
  public List<AspectRowSummary> findByParams(
      @Nonnull OperationContext opContext,
      Map<String, String> systemMetaParams,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size) {
    StringBuilder sql =
        new StringBuilder("SELECT document::text FROM ").append(qualifiedTable()).append(" WHERE ");
    List<String> cond = new ArrayList<>();
    List<String> vals = new ArrayList<>();
    for (Map.Entry<String, String> e : systemMetaParams.entrySet()) {
      String col = columnForSqlParamKey(e.getKey());
      if (col != null) {
        cond.add(col + " = ?");
        vals.add(e.getValue());
      }
    }
    if (vals.isEmpty()) {
      return Collections.emptyList();
    }
    if (!includeSoftDeleted) {
      cond.add("NOT COALESCE(removed, false)");
    }
    sql.append(String.join(" AND ", cond));
    sql.append(" ORDER BY urn, aspect LIMIT ? OFFSET ?");
    int limit = ConfigUtils.applyLimit(systemMetadataServiceConfig, size);
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql.toString())) {
      int i = 1;
      for (String v : vals) {
        ps.setString(i++, v);
      }
      ps.setInt(i++, limit);
      ps.setInt(i, from);
      try (ResultSet rs = ps.executeQuery()) {
        List<AspectRowSummary> out = new ArrayList<>();
        while (rs.next()) {
          JsonNode doc = objectMapper(opContext).readTree(rs.getString(1));
          out.add(rowToSummary(doc));
        }
        return out;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<AspectRowSummary> findByRegistry(
      @Nonnull OperationContext opContext,
      String registryName,
      String registryVersion,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size) {
    Map<String, String> registryParams = new HashMap<>();
    registryParams.put(REGISTRY_NAME_JSON, registryName);
    registryParams.put(REGISTRY_VERSION_JSON, registryVersion);
    return findByParams(opContext, registryParams, includeSoftDeleted, from, size);
  }

  @Override
  public List<IngestionRunSummary> listRuns(
      @Nonnull OperationContext opContext,
      Integer pageOffset,
      Integer pageSize,
      boolean includeSoftDeleted) {
    int offset = pageOffset != null ? pageOffset : 0;
    int limit = ConfigUtils.applyLimit(systemMetadataServiceConfig, pageSize);
    /*
     Approximate aggregated runs: grouping by run_id, filter out runs where every row is removed
    when includeSoftDeleted is false.
    */
    String having =
        includeSoftDeleted
            ? ""
            : " HAVING SUM(CASE WHEN NOT COALESCE(removed,false) THEN 1 ELSE 0 END) > 0";
    String sql =
        "SELECT run_id, MAX(last_updated) AS max_ts, COUNT(*) AS doc_count FROM "
            + qualifiedTable()
            + " WHERE run_id IS NOT NULL AND trim(run_id) <> ''"
            + " GROUP BY run_id"
            + having
            + " ORDER BY max_ts DESC NULLS LAST LIMIT ? OFFSET ?";
    List<IngestionRunSummary> result = new ArrayList<>();
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setInt(1, limit);
      ps.setInt(2, offset);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          IngestionRunSummary row = new IngestionRunSummary();
          row.setRunId(rs.getString(1));
          row.setTimestamp(rs.getLong(2));
          row.setRows(rs.getLong(3));
          result.add(row);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  @Override
  public List<AspectRowSummary> findAspectsByUrn(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<String> aspects,
      boolean includeSoftDeleted) {
    String ph = aspects.stream().map(a -> "?").collect(Collectors.joining(","));
    StringBuilder sql =
        new StringBuilder("SELECT document::text FROM ")
            .append(qualifiedTable())
            .append(" WHERE urn = ? AND aspect IN (")
            .append(ph)
            .append(")");
    if (!includeSoftDeleted) {
      sql.append(" AND NOT COALESCE(removed, false)");
    }
    sql.append(" ORDER BY aspect");

    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql.toString())) {
      int ix = 1;
      ps.setString(ix++, urn.toString());
      for (String a : aspects) {
        ps.setString(ix++, a);
      }
      try (ResultSet rs = ps.executeQuery()) {
        List<AspectRowSummary> out = new ArrayList<>();
        while (rs.next()) {
          JsonNode doc = objectMapper(opContext).readTree(rs.getString(1));
          out.add(rowToSummary(doc));
        }
        return out;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear(@Nonnull OperationContext opContext) {
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE " + qualifiedTable());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<Urn, Map<String, Map<String, Object>>> raw(
      OperationContext opContext, Map<String, Set<String>> urnAspects) {

    if (urnAspects == null || urnAspects.isEmpty()) {
      return Collections.emptyMap();
    }

    List<String> docIds = new ArrayList<>();
    for (Map.Entry<String, Set<String>> entry : urnAspects.entrySet()) {
      Set<String> aspects = entry.getValue();
      if (aspects != null && !aspects.isEmpty()) {
        for (String aspect : aspects) {
          docIds.add(toDocId(entry.getKey(), aspect));
        }
      }
    }
    if (docIds.isEmpty()) {
      return Collections.emptyMap();
    }

    String ph = docIds.stream().map(d -> "?").collect(Collectors.joining(","));
    String sql =
        "SELECT urn, aspect, document::text FROM "
            + qualifiedTable()
            + " WHERE doc_id IN ("
            + ph
            + ")";

    Map<Urn, Map<String, Map<String, Object>>> result = new HashMap<>();
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      for (int i = 0; i < docIds.size(); i++) {
        ps.setString(i + 1, docIds.get(i));
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Urn urn = UrnUtils.getUrn(rs.getString(1));
          String aspectName = rs.getString(2);
          String docJson = rs.getString(3);
          @SuppressWarnings("unchecked")
          Map<String, Object> source = objectMapper(opContext).readValue(docJson, Map.class);
          result.computeIfAbsent(urn, k -> new HashMap<>()).put(aspectName, source);
        }
      }
      return result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  @Override
  public KeyAspectCount countByKeyAspect(
      @Nonnull OperationContext opContext, @Nonnull String keyAspectName) {
    String sql =
        "SELECT "
            + "COUNT(*) FILTER (WHERE NOT COALESCE(removed, false)) AS active_count, "
            + "COUNT(*) FILTER (WHERE COALESCE(removed, false)) AS soft_deleted_count "
            + "FROM "
            + qualifiedTable()
            + " WHERE aspect = ?";
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, keyAspectName);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return new KeyAspectCount(rs.getLong("active_count"), rs.getLong("soft_deleted_count"));
        }
        return KeyAspectCount.empty();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  @Override
  public Map<String, KeyAspectCount> countByKeyAspects(
      @Nonnull OperationContext opContext, @Nonnull List<String> keyAspectNames) {
    if (keyAspectNames.isEmpty()) {
      return Collections.emptyMap();
    }
    String ph = keyAspectNames.stream().map(a -> "?").collect(Collectors.joining(","));
    String sql =
        "SELECT aspect, "
            + "COUNT(*) FILTER (WHERE NOT COALESCE(removed, false)) AS active_count, "
            + "COUNT(*) FILTER (WHERE COALESCE(removed, false)) AS soft_deleted_count "
            + "FROM "
            + qualifiedTable()
            + " WHERE aspect IN ("
            + ph
            + ") GROUP BY aspect";
    Map<String, KeyAspectCount> result = new HashMap<>();
    for (String keyAspectName : keyAspectNames) {
      result.put(keyAspectName, KeyAspectCount.empty());
    }
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      int i = 1;
      for (String keyAspectName : keyAspectNames) {
        ps.setString(i++, keyAspectName);
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          result.put(
              rs.getString("aspect"),
              new KeyAspectCount(rs.getLong("active_count"), rs.getLong("soft_deleted_count")));
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}
