package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.MCL_HEADER_DATABASE_ASPECT_VERSION;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.write.EntitySearchWriteSink;
import com.linkedin.metadata.service.search.CombinedSearchDocumentBuilder;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * PostgreSQL entity search (pgSearch SqlSetup) as a separate {@link UpdateIndicesStrategy}.
 *
 * <p>Writes use the same <strong>combined search document</strong> shape and batching approach as
 * {@link UpdateIndicesV3Strategy} ({@link CombinedSearchDocumentBuilder}) — one row per (urn,
 * search_group). There is no legacy per-aspect (ES V2-style) Postgres path.
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresEntitySearchStrategy implements UpdateIndicesStrategy {

  @Nonnull private final PostgresSqlSetupProperties postgresSqlSetupProperties;
  @Nonnull private final EntitySearchWriteSink entitySearchWriteSink;
  @Nonnull private final SearchDocumentTransformer searchDocumentTransformer;

  @Override
  public void processBatch(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, List<MCLItem>> groupedEvents,
      boolean structuredPropertiesHookEnabled) {

    if (!isEnabled() || groupedEvents.isEmpty()) {
      return;
    }

    CombinedSearchDocumentBuilder builder =
        new CombinedSearchDocumentBuilder(searchDocumentTransformer);
    for (Map.Entry<Urn, List<MCLItem>> entry : groupedEvents.entrySet()) {
      processUrnGroup(opContext, entry.getKey(), entry.getValue(), builder);
    }
  }

  private void processUrnGroup(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> events,
      @Nonnull CombinedSearchDocumentBuilder builder) {

    boolean hasKeyAspectDeletion =
        events.stream()
            .anyMatch(
                event -> {
                  try {
                    if (event.getChangeType() == ChangeType.DELETE) {
                      Pair<EntitySpec, AspectSpec> specPair =
                          UpdateIndicesUtil.extractSpecPair(event);
                      return UpdateIndicesUtil.isDeletingKey(specPair);
                    }
                    return false;
                  } catch (Exception e) {
                    log.error(
                        "Postgres entity search: error checking key deletion for {}: {}",
                        event.getAspectName(),
                        e.getMessage(),
                        e);
                    return false;
                  }
                });

    if (hasKeyAspectDeletion) {
      String searchGroup = events.get(0).getEntitySpec().getSearchGroup();
      if (searchGroup == null) {
        log.error(
            "Postgres entity search: key aspect deletion but search group is null for URN: {}",
            urn);
        return;
      }
      entitySearchWriteSink.deleteDocumentBySearchGroup(opContext, searchGroup, urn.toString());
      return;
    }

    List<MCLItem> indexingEvents =
        events.stream()
            .filter(PostgresEntitySearchStrategy::isDatabaseLatestAspectRowForPgSearch)
            .collect(Collectors.toList());
    if (indexingEvents.isEmpty()) {
      return;
    }

    ObjectNode combinedDocument = builder.buildCombinedDocument(opContext, urn, indexingEvents);
    if (combinedDocument == null) {
      return;
    }

    String searchGroup = indexingEvents.get(0).getEntitySpec().getSearchGroup();
    if (searchGroup == null) {
      log.error("Postgres entity search: search group is null for URN: {}", urn);
      return;
    }

    entitySearchWriteSink.upsertDocumentBySearchGroup(
        opContext, searchGroup, combinedDocument.toString(), urn.toString());

    String docId = opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn);
    List<String> distinctRunIds =
        indexingEvents.stream()
            .filter(e -> e.getSystemMetadata() != null && e.getSystemMetadata().hasRunId())
            .map(e -> e.getSystemMetadata().getRunId())
            .distinct()
            .collect(Collectors.toList());
    for (String runId : distinctRunIds) {
      entitySearchWriteSink.appendRunIdBySearchGroup(opContext, searchGroup, docId, urn, runId);
    }
  }

  @Override
  public Collection<MappingsBuilder.IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext) {
    return Collections.emptyList();
  }

  @Override
  public Collection<MappingsBuilder.IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {
    return Collections.emptyList();
  }

  @Override
  public void updateIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull EntitySpec entitySpec,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull Object newValue,
      @Nullable Object oldValue) {
    // Mapping updates target Elasticsearch; pgSearch rows track documents only.
  }

  @Override
  public boolean isEnabled() {
    return postgresSqlSetupProperties.getPgSearch().getEntity().isEnabled();
  }

  /**
   * pgSearch mirrors {@code metadata_aspect} rows with {@link ASPECT_LATEST_VERSION} (see {@link
   * com.linkedin.metadata.entity.ebean.EbeanAspectDao} restore/stream filters). When {@link
   * com.linkedin.mxe.MetadataChangeLog} carries {@link MCL_HEADER_DATABASE_ASPECT_VERSION}, only
   * events for the latest row update the store; absence of the header keeps backward compatibility
   * (e.g. timeseries MCLs built without {@link com.linkedin.metadata.entity.UpdateAspectResult}).
   */
  static boolean isDatabaseLatestAspectRowForPgSearch(@Nonnull MCLItem event) {
    if (event.getChangeType() == ChangeType.DELETE) {
      return true;
    }
    MetadataChangeLog mcl = event.getMetadataChangeLog();
    if (!mcl.hasHeaders() || mcl.getHeaders() == null) {
      return true;
    }
    String rowVersion = mcl.getHeaders().get(MCL_HEADER_DATABASE_ASPECT_VERSION);
    if (rowVersion == null) {
      return true;
    }
    try {
      return Long.parseLong(rowVersion.trim()) == ASPECT_LATEST_VERSION;
    } catch (NumberFormatException e) {
      log.warn(
          "Postgres entity search: invalid {} header '{}', skipping MCL for urn={} aspect={}",
          MCL_HEADER_DATABASE_ASPECT_VERSION,
          rowVersion,
          event.getUrn(),
          event.getAspectName());
      return false;
    }
  }
}
