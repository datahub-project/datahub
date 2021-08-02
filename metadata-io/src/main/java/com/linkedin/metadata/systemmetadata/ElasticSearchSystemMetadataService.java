package com.linkedin.metadata.systemmetadata;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.SystemMetadata;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
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

  private String toDocument(SystemMetadata systemMetadata, String urn, String aspect) {
    final ObjectNode document = JsonNodeFactory.instance.objectNode();

    document.put("urn", urn);
    document.put("aspect", aspect);
    document.put("runId", systemMetadata.getRunId());
    document.put("lastUpdated", systemMetadata.getLastObserved());

    return document.toString();
  }

  private String toDocId(@Nonnull final String urn, @Nonnull final String aspect) {
    String rawDocId =
        urn + DOC_DELIMETER + aspect;

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
  public void insert(SystemMetadata systemMetadata, String urn, String aspect) {
    String document = toDocument(systemMetadata, urn, aspect);
    String docId = toDocId(urn, aspect);
    _esDAO.upsertDocument(docId, document);
  }

  @Override
  public List<AspectRowSummary> findByRunId(String runId) {
    SearchHits hits = _esDAO.findByRunId(runId).getHits();
    List<AspectRowSummary> summaries = Arrays.stream(hits.getHits()).map(hit -> {
      Map<String, Object> values = hit.getSourceAsMap();
      AspectRowSummary summary = new AspectRowSummary();
      summary.setRunId((String) values.get("runId"));
      summary.setAspectName((String) values.get("aspect"));
      summary.setUrn((String) values.get("urn"));
      Object timestamp = values.get("lastUpdated");
      if (timestamp instanceof Long) {
        summary.setTimestamp((Long) timestamp);
      } else if (timestamp instanceof Integer) {
        summary.setTimestamp(Long.valueOf((Integer) timestamp));
      }
      summary.setKeyAspect(((String) values.get("aspect")).endsWith("Key"));
      return summary;
    }).collect(Collectors.toList());

    return summaries;
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
    boolean exists = false;
    try {
      exists = searchClient.indices().exists(
          new GetIndexRequest(_indexConvention.getIndexName(INDEX_NAME)), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("ERROR: Failed to set up elasticsearch system metadata index. Could not check if the index exists");
      e.printStackTrace();
      return;
    }

    // If index doesn't exist, create index
    if (!exists) {
      log.info("Elastic System Metadata Index does not exist. Creating.");
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(_indexConvention.getIndexName(INDEX_NAME));

      createIndexRequest.mapping(SystemMetadataMappingsBuilder.getMappings());

      try {
        searchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
      } catch (IOException e) {
        log.error("ERROR: Failed to set up elasticsearch system metadata index. Could not create the index.");
        e.printStackTrace();
        return;
      }

      log.info("Successfully Created Elastic System Metadata Index");
    }

    return;
  }

  @Override
  public void clear() {
    DeleteByQueryRequest deleteRequest =
        new DeleteByQueryRequest(_indexConvention.getIndexName(INDEX_NAME)).setQuery(QueryBuilders.matchAllQuery());
    try {
      searchClient.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Failed to clear graph service: {}", e.toString());
    }
  }
}
