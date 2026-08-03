package com.linkedin.metadata.search.postgres;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.search.write.AbstractEntitySearchWriteSink;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * JDBC dual-write for SqlSetup {@code {prefix}_search_row} (see {@code
 * datahub-upgrade/src/main/resources/sqlsetup/pgsearch_entity/}). Rows are keyed by {@code urn}
 * only (one row per entity); {@code search_group} is stored for filtering and updated on conflict.
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresEntitySearchWriteSink extends AbstractEntitySearchWriteSink {

  @Nonnull private final Database database;
  @Nonnull private final PostgresSqlSetupProperties postgresSqlSetupProperties;

  @Override
  public void upsertDocumentBySearchGroup(
      @Nonnull OperationContext opContext,
      @Nonnull String searchGroup,
      @Nonnull String documentJson,
      @Nonnull String urn) {
    upsertSearchDocumentJson(opContext, urn, searchGroup, documentJson);
  }

  @Override
  public void deleteDocumentBySearchGroup(
      @Nonnull OperationContext opContext, @Nonnull String searchGroup, @Nonnull String urn) {
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgSearchEntityTablePrefix();
    String table = schema + "." + prefix + "_search_row";
    String sql = "DELETE FROM " + table + " WHERE urn = ?";
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, urn);
        ps.executeUpdate();
      }
    } catch (SQLException e) {
      log.error(
          "Postgres entity search dual-write delete failed for urn={} searchGroup={}: {}",
          urn,
          searchGroup,
          e.getMessage(),
          e);
    }
  }

  @Override
  public void appendRunId(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nullable String runId) {
    if (runId == null || runId.isBlank()) {
      return;
    }
    String searchGroup =
        opContext.getEntityRegistry().getEntitySpec(urn.getEntityType()).getSearchGroup();
    if (searchGroup == null) {
      log.debug(
          "Postgres appendRunId: entity {} has no searchGroup; skipping", urn.getEntityType());
      return;
    }
    String docId = opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn);
    appendRunIdBySearchGroup(opContext, searchGroup, docId, urn, runId);
  }

  @Override
  public void appendRunIdBySearchGroup(
      @Nonnull OperationContext opContext,
      @Nonnull String searchGroup,
      @Nonnull String docId,
      @Nonnull Urn urn,
      @Nullable String runId) {
    if (runId == null || runId.isBlank()) {
      return;
    }
    String urnStr = urn.toString();
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgSearchEntityTablePrefix();
    String table = schema + "." + prefix + "_search_row";
    String selectSql = "SELECT document::text FROM " + table + " WHERE urn = ?";
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      String existingJson = null;
      try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
        ps.setString(1, urnStr);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            existingJson = rs.getString(1);
          }
        }
      }
      String merged =
          AbstractEntitySearchWriteSink.mergeRunIdIntoDocumentJson(existingJson, urnStr, runId);
      upsertSearchDocumentJson(opContext, urnStr, searchGroup, merged);
    } catch (SQLException e) {
      log.error(
          "Postgres entity search appendRunId failed for urn={} searchGroup={}: {}",
          urnStr,
          searchGroup,
          e.getMessage(),
          e);
    }
  }

  private void upsertSearchDocumentJson(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String searchGroup,
      @Nonnull String documentJson) {
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgSearchEntityTablePrefix();
    String table = schema + "." + prefix + "_search_row";
    String lang = defaultFulltextLanguage();
    int tierCount = effectiveTierTsvectorColumnCount();
    String[] tierTexts =
        PostgresSearchTierTextAggregator.deriveTierPlainTexts(
            opContext.getEntityRegistry(), documentJson, tierCount);
    String extrasJson = PostgresSearchExtrasJson.buildSearchExtrasPayload(documentJson);
    String entityType = PostgresSearchEntityType.extractEntityType(documentJson);
    boolean vectorEnabled =
        postgresSqlSetupProperties.getPgSearch().getEntity().getVector().isEnabled();

    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      String existingSystemMetadataJson = null;
      try (PreparedStatement sel =
          conn.prepareStatement("SELECT systemmetadata::text FROM " + table + " WHERE urn = ?")) {
        sel.setString(1, urn);
        try (ResultSet rs = sel.executeQuery()) {
          if (rs.next()) {
            existingSystemMetadataJson = rs.getString(1);
          }
        }
      }
      String systemMetadataJson =
          PostgresSearchSystemMetadataJson.mergeAspectSystemMetadataForUpsert(
              existingSystemMetadataJson, documentJson);

      StringJoiner columns = new StringJoiner(", ");
      columns.add("urn");
      columns.add("search_group");
      columns.add("entity_type");
      columns.add("document");
      columns.add("search_extras");
      columns.add("systemmetadata");
      StringJoiner valuePlaceholders = new StringJoiner(", ");
      valuePlaceholders.add("?");
      valuePlaceholders.add("?");
      valuePlaceholders.add("?");
      valuePlaceholders.add("?::jsonb");
      valuePlaceholders.add(extrasJson == null ? "NULL" : "?::jsonb");
      valuePlaceholders.add(systemMetadataJson == null ? "NULL" : "?::jsonb");

      StringJoiner updates = new StringJoiner(", ");
      updates.add("search_group = EXCLUDED.search_group");
      updates.add("entity_type = EXCLUDED.entity_type");
      updates.add("document = EXCLUDED.document");
      updates.add("search_extras = EXCLUDED.search_extras");
      updates.add("systemmetadata = EXCLUDED.systemmetadata");

      for (int t = 1; t <= tierCount; t++) {
        String textCol = PostgresSqlSetupProperties.searchTextTierColumnName(t);
        String vecCol = PostgresSqlSetupProperties.searchVectorTierColumnName(t);
        columns.add(textCol);
        columns.add(vecCol);
        valuePlaceholders.add("?");
        valuePlaceholders.add("to_tsvector(?::regconfig, ?)");
        updates.add(textCol + " = EXCLUDED." + textCol);
        updates.add(vecCol + " = EXCLUDED." + vecCol);
      }
      if (vectorEnabled) {
        for (int t = 1; t <= tierCount; t++) {
          String embCol = PostgresSqlSetupProperties.embeddingTierColumnName(t);
          columns.add(embCol);
          valuePlaceholders.add("NULL");
          updates.add(embCol + " = EXCLUDED." + embCol);
        }
      }

      String sql =
          "INSERT INTO "
              + table
              + " ("
              + columns
              + ") VALUES ("
              + valuePlaceholders
              + ") ON CONFLICT (urn) DO UPDATE SET "
              + updates;

      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int i = 1;
        ps.setString(i++, urn);
        ps.setString(i++, searchGroup);
        ps.setString(i++, entityType);
        ps.setObject(i++, documentJson, Types.OTHER);
        if (extrasJson != null) {
          ps.setObject(i++, extrasJson, Types.OTHER);
        }
        if (systemMetadataJson != null) {
          ps.setObject(i++, systemMetadataJson, Types.OTHER);
        }
        for (int t = 0; t < tierCount; t++) {
          ps.setString(i++, tierTexts[t]);
          ps.setString(i++, lang);
          ps.setString(i++, tierTexts[t]);
        }
        ps.executeUpdate();
      }
    } catch (SQLException e) {
      log.error(
          "Postgres entity search dual-write upsert failed for urn={} searchGroup={}: {}",
          urn,
          searchGroup,
          e.getMessage(),
          e);
    }
  }

  private int effectiveTierTsvectorColumnCount() {
    int n =
        postgresSqlSetupProperties
            .getPgSearch()
            .getEntity()
            .getFulltext()
            .getTierTsvectorColumnCount();
    if (n < 1) {
      return 1;
    }
    return Math.min(n, 32);
  }

  @Nonnull
  private String defaultFulltextLanguage() {
    String raw =
        postgresSqlSetupProperties.getPgSearch().getEntity().getFulltext().getDefaultLanguage();
    if (raw == null || raw.isBlank()) {
      return "english";
    }
    return raw.trim();
  }
}
