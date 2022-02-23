package com.linkedin.metadata.systemmetadata;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.SystemMetadata;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;


@Slf4j
@RequiredArgsConstructor
public class ElasticSearchSystemMetadataService implements SystemMetadataService {

  private final RestHighLevelClient searchClient;
  private final IndexConvention _indexConvention;
  private final ESSystemMetadataDAO _esDAO;

  private static final String DOC_DELIMETER = "--";
  public static final String INDEX_NAME = "system_metadata_service_v1";
  private static final String FIELD_URN = "urn";
  private static final String FIELD_ASPECT = "aspect";
  private static final String FIELD_RUNID = "runId";
  private static final String FIELD_LAST_UPDATED = "lastUpdated";
  private static final String FIELD_REGISTRY_NAME = "registryName";
  private static final String FIELD_REGISTRY_VERSION = "registryVersion";
  private static final Set<String> INDEX_FIELD_SET = new HashSet<>(
      Arrays.asList(FIELD_URN, FIELD_ASPECT, FIELD_RUNID, FIELD_LAST_UPDATED, FIELD_REGISTRY_NAME,
          FIELD_REGISTRY_VERSION));

  private String toDocument(SystemMetadata systemMetadata, String urn, String aspect) {
    final ObjectNode document = JsonNodeFactory.instance.objectNode();

    document.put("urn", urn);
    document.put("aspect", aspect);
    document.put("runId", systemMetadata.getRunId());
    document.put("lastUpdated", systemMetadata.getLastObserved());
    document.put("registryName", systemMetadata.getRegistryName());
    document.put("registryVersion", systemMetadata.getRegistryVersion());
    return document.toString();
  }

  private String toDocId(@Nonnull final String urn, @Nonnull final String aspect) {
    String rawDocId = urn + DOC_DELIMETER + aspect;

    try {
      byte[] bytesOfRawDocID = rawDocId.getBytes("UTF-8");
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] thedigest = md.digest(bytesOfRawDocID);
      return new String(thedigest, StandardCharsets.US_ASCII);
    } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
      e.printStackTrace();
      return rawDocId;
    }
  }

  @Override
  public Boolean delete(String urn, String aspect) {
    String docId = toDocId(urn, aspect);
    DeleteResponse response = _esDAO.deleteByDocId(docId);
    return response.status().getStatus() >= 200 && response.status().getStatus() < 300;
  }

  @Override
  public void deleteUrn(String urn) {
    _esDAO.deleteByUrn(urn);
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
  public List<AspectRowSummary> findByRunId(String runId) {
    return findByParams(Collections.singletonMap(FIELD_RUNID, runId));
  }

  private List<AspectRowSummary> findByParams(Map<String, String> systemMetaParams) {
    SearchResponse searchResponse = _esDAO.findByParams(systemMetaParams);
    if (searchResponse != null) {
      SearchHits hits = searchResponse.getHits();
      List<AspectRowSummary> summaries = Arrays.stream(hits.getHits()).map(hit -> {
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
      }).collect(Collectors.toList());
      return summaries;
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public List<AspectRowSummary> findByRegistry(String registryName, String registryVersion) {
    Map<String, String> registryParams = new HashMap<>();
    registryParams.put(FIELD_REGISTRY_NAME, registryName);
    registryParams.put(FIELD_REGISTRY_VERSION, registryVersion);
    return findByParams(registryParams);
  }

  @Override
  public List<IngestionRunSummary> listRuns(Integer pageOffset, Integer pageSize) {
    SearchResponse response = _esDAO.findRuns(pageOffset, pageSize);
    List<? extends Terms.Bucket> buckets = ((ParsedStringTerms) response.getAggregations().get("runId")).getBuckets();
    // TODO(gabe-lyons): add sample urns
    return buckets.stream().map(bucket -> {
      IngestionRunSummary entry = new IngestionRunSummary();
      entry.setRunId(bucket.getKeyAsString());
      entry.setTimestamp((long) ((ParsedMax) bucket.getAggregations().get("maxTimestamp")).getValue());
      entry.setRows(bucket.getDocCount());
      return entry;
    }).collect(Collectors.toList());
  }

  @Override
  public void configure() {
    log.info("Setting up system metadata index");
    IndexBuilder ib = new IndexBuilder(this.searchClient, _indexConvention.getIndexName(INDEX_NAME),
        SystemMetadataMappingsBuilder.getMappings(), Collections.emptyMap());
    try {
      ib.buildIndex();
    } catch (IOException ie) {
      throw new RuntimeException("Could not configure system metadata index", ie);
    }
  }

  @Override
  public void clear() {
    DeleteByQueryRequest deleteRequest =
        new DeleteByQueryRequest(_indexConvention.getIndexName(INDEX_NAME)).setQuery(QueryBuilders.matchAllQuery());
    try {
      searchClient.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Failed to clear system metadata service: {}", e.toString());
    }
  }
}
