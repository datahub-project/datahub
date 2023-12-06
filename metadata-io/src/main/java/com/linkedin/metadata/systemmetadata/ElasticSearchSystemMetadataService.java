package com.linkedin.metadata.systemmetadata;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.SystemMetadata;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilter;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.ParsedMax;

@Slf4j
@RequiredArgsConstructor
public class ElasticSearchSystemMetadataService
    implements SystemMetadataService, ElasticSearchIndexed {

  private final ESBulkProcessor _esBulkProcessor;
  private final IndexConvention _indexConvention;
  private final ESSystemMetadataDAO _esDAO;
  private final ESIndexBuilder _indexBuilder;

  private static final String DOC_DELIMETER = "--";
  public static final String INDEX_NAME = "system_metadata_service_v1";
  private static final String FIELD_URN = "urn";
  private static final String FIELD_ASPECT = "aspect";
  private static final String FIELD_RUNID = "runId";
  private static final String FIELD_LAST_UPDATED = "lastUpdated";
  private static final String FIELD_REGISTRY_NAME = "registryName";
  private static final String FIELD_REGISTRY_VERSION = "registryVersion";
  private static final Set<String> INDEX_FIELD_SET =
      new HashSet<>(
          Arrays.asList(
              FIELD_URN,
              FIELD_ASPECT,
              FIELD_RUNID,
              FIELD_LAST_UPDATED,
              FIELD_REGISTRY_NAME,
              FIELD_REGISTRY_VERSION));

  private String toDocument(SystemMetadata systemMetadata, String urn, String aspect) {
    final ObjectNode document = JsonNodeFactory.instance.objectNode();

    document.put("urn", urn);
    document.put("aspect", aspect);
    document.put("runId", systemMetadata.getRunId());
    document.put("lastUpdated", systemMetadata.getLastObserved());
    document.put("registryName", systemMetadata.getRegistryName());
    document.put("registryVersion", systemMetadata.getRegistryVersion());
    document.put("removed", false);
    return document.toString();
  }

  private String toDocId(@Nonnull final String urn, @Nonnull final String aspect) {
    String rawDocId = urn + DOC_DELIMETER + aspect;

    try {
      byte[] bytesOfRawDocID = rawDocId.getBytes(StandardCharsets.UTF_8);
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] thedigest = md.digest(bytesOfRawDocID);
      return Base64.getEncoder().encodeToString(thedigest);
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      return rawDocId;
    }
  }

  @Override
  public void deleteAspect(String urn, String aspect) {
    _esDAO.deleteByUrnAspect(urn, aspect);
  }

  @Override
  public Optional<GetTaskResponse> getTaskStatus(@Nonnull String nodeId, long taskId) {
    return _esDAO.getTaskStatus(nodeId, taskId);
  }

  @Override
  public void deleteUrn(String urn) {
    _esDAO.deleteByUrn(urn);
  }

  @Override
  public void setDocStatus(String urn, boolean removed) {
    // searchBy findByParams
    // If status.removed -> false (from removed to not removed) --> get soft deleted entities.
    // If status.removed -> true (from not removed to removed) --> do not get soft deleted entities.
    final List<AspectRowSummary> aspectList =
        findByParams(ImmutableMap.of("urn", urn), !removed, 0, ESUtils.MAX_RESULT_SIZE);
    // for each -> toDocId and set removed to true for all
    aspectList.forEach(
        aspect -> {
          final String docId = toDocId(aspect.getUrn(), aspect.getAspectName());
          final ObjectNode document = JsonNodeFactory.instance.objectNode();
          document.put("removed", removed);
          _esDAO.upsertDocument(docId, document.toString());
        });
  }

  @Override
  public void insert(@Nullable SystemMetadata systemMetadata, String urn, String aspect) {
    if (systemMetadata == null) {
      return;
    }

    String docId = toDocId(urn, aspect);

    String document = toDocument(systemMetadata, urn, aspect);
    _esDAO.upsertDocument(docId, document);
  }

  @Override
  public List<AspectRowSummary> findByRunId(
      String runId, boolean includeSoftDeleted, int from, int size) {
    return findByParams(
        Collections.singletonMap(FIELD_RUNID, runId), includeSoftDeleted, from, size);
  }

  @Override
  public List<AspectRowSummary> findByUrn(
      String urn, boolean includeSoftDeleted, int from, int size) {
    return findByParams(Collections.singletonMap(FIELD_URN, urn), includeSoftDeleted, from, size);
  }

  @Override
  public List<AspectRowSummary> findByParams(
      Map<String, String> systemMetaParams, boolean includeSoftDeleted, int from, int size) {
    SearchResponse searchResponse =
        _esDAO.findByParams(systemMetaParams, includeSoftDeleted, from, size);
    if (searchResponse != null) {
      SearchHits hits = searchResponse.getHits();
      List<AspectRowSummary> summaries =
          Arrays.stream(hits.getHits())
              .map(
                  hit -> {
                    Map<String, Object> values = hit.getSourceAsMap();
                    AspectRowSummary summary = new AspectRowSummary();
                    summary.setRunId((String) values.get(FIELD_RUNID));
                    summary.setAspectName((String) values.get(FIELD_ASPECT));
                    summary.setUrn((String) values.get(FIELD_URN));
                    Object timestamp = values.get(FIELD_LAST_UPDATED);
                    if (timestamp instanceof Long) {
                      summary.setTimestamp((Long) timestamp);
                    } else if (timestamp instanceof Integer) {
                      summary.setTimestamp(Long.valueOf((Integer) timestamp));
                    }
                    summary.setKeyAspect(((String) values.get(FIELD_ASPECT)).endsWith("Key"));
                    return summary;
                  })
              .collect(Collectors.toList());
      return summaries;
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public List<AspectRowSummary> findByRegistry(
      String registryName, String registryVersion, boolean includeSoftDeleted, int from, int size) {
    Map<String, String> registryParams = new HashMap<>();
    registryParams.put(FIELD_REGISTRY_NAME, registryName);
    registryParams.put(FIELD_REGISTRY_VERSION, registryVersion);
    return findByParams(registryParams, includeSoftDeleted, from, size);
  }

  @Override
  public List<IngestionRunSummary> listRuns(
      Integer pageOffset, Integer pageSize, boolean includeSoftDeleted) {
    SearchResponse response = _esDAO.findRuns(pageOffset, pageSize);
    List<? extends Terms.Bucket> buckets =
        ((ParsedStringTerms) response.getAggregations().get("runId")).getBuckets();

    if (!includeSoftDeleted) {
      buckets.removeIf(
          bucket -> {
            long totalDocs = bucket.getDocCount();
            long softDeletedDocs =
                ((ParsedFilter) bucket.getAggregations().get("removed")).getDocCount();
            return totalDocs == softDeletedDocs;
          });
    }

    // TODO(gabe-lyons): add sample urns
    return buckets.stream()
        .map(
            bucket -> {
              IngestionRunSummary entry = new IngestionRunSummary();
              entry.setRunId(bucket.getKeyAsString());
              entry.setTimestamp(
                  (long) ((ParsedMax) bucket.getAggregations().get("maxTimestamp")).getValue());
              entry.setRows(bucket.getDocCount());
              return entry;
            })
        .collect(Collectors.toList());
  }

  @Override
  public void configure() {
    log.info("Setting up system metadata index");
    try {
      for (ReindexConfig config : buildReindexConfigs()) {
        _indexBuilder.buildIndex(config);
      }
    } catch (IOException ie) {
      throw new RuntimeException("Could not configure system metadata index", ie);
    }
  }

  @Override
  public List<ReindexConfig> buildReindexConfigs() throws IOException {
    return List.of(
        _indexBuilder.buildReindexState(
            _indexConvention.getIndexName(INDEX_NAME),
            SystemMetadataMappingsBuilder.getMappings(),
            Collections.emptyMap()));
  }

  @Override
  public void reindexAll() {
    configure();
  }

  @VisibleForTesting
  @Override
  public void clear() {
    _esBulkProcessor.deleteByQuery(
        QueryBuilders.matchAllQuery(), true, _indexConvention.getIndexName(INDEX_NAME));
  }
}
