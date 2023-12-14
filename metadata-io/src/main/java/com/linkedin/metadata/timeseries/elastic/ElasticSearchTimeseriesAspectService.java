package com.linkedin.metadata.timeseries.elastic;

import static com.linkedin.metadata.Constants.*;

import com.codahale.metrics.Timer;
import com.datahub.util.RecordUtils;
import com.datahub.util.exception.ESQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.timeseries.BatchWriteOperationsOptions;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.TimeseriesAspectIndexBuilders;
import com.linkedin.metadata.timeseries.elastic.query.ESAggregatedStatsDAO;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.tasks.TaskSubmissionResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

@Slf4j
public class ElasticSearchTimeseriesAspectService
    implements TimeseriesAspectService, ElasticSearchIndexed {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  private static final String TIMESTAMP_FIELD = "timestampMillis";
  private static final String EVENT_FIELD = "event";
  private static final Integer DEFAULT_LIMIT = 10000;

  private final IndexConvention _indexConvention;
  private final ESBulkProcessor _bulkProcessor;
  private final int _numRetries;
  private final TimeseriesAspectIndexBuilders _indexBuilders;
  private final RestHighLevelClient _searchClient;
  private final ESAggregatedStatsDAO _esAggregatedStatsDAO;
  private final EntityRegistry _entityRegistry;

  public ElasticSearchTimeseriesAspectService(
      @Nonnull RestHighLevelClient searchClient,
      @Nonnull IndexConvention indexConvention,
      @Nonnull TimeseriesAspectIndexBuilders indexBuilders,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull ESBulkProcessor bulkProcessor,
      int numRetries) {
    _indexConvention = indexConvention;
    _indexBuilders = indexBuilders;
    _searchClient = searchClient;
    _bulkProcessor = bulkProcessor;
    _entityRegistry = entityRegistry;
    _numRetries = numRetries;

    _esAggregatedStatsDAO = new ESAggregatedStatsDAO(indexConvention, searchClient, entityRegistry);
  }

  private static EnvelopedAspect parseDocument(@Nonnull SearchHit doc) {
    Map<String, Object> docFields = doc.getSourceAsMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    Object event = docFields.get(EVENT_FIELD);
    GenericAspect genericAspect;
    try {
      genericAspect =
          new GenericAspect()
              .setValue(
                  ByteString.unsafeWrap(
                      OBJECT_MAPPER.writeValueAsString(event).getBytes(StandardCharsets.UTF_8)));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          "Failed to deserialize event from the timeseries aspect index: " + e);
    }
    genericAspect.setContentType("application/json");
    envelopedAspect.setAspect(genericAspect);
    Object systemMetadata = docFields.get("systemMetadata");
    if (systemMetadata != null) {
      try {
        envelopedAspect.setSystemMetadata(
            RecordUtils.toRecordTemplate(
                SystemMetadata.class, OBJECT_MAPPER.writeValueAsString(systemMetadata)));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(
            "Failed to deserialize system metadata from the timeseries aspect index: " + e);
      }
    }

    return envelopedAspect;
  }

  @Override
  public void configure() {
    _indexBuilders.reindexAll();
  }

  @Override
  public List<ReindexConfig> buildReindexConfigs() {
    return _indexBuilders.buildReindexConfigs();
  }

  public String reindexAsync(
      String index, @Nullable QueryBuilder filterQuery, BatchWriteOperationsOptions options)
      throws Exception {
    return _indexBuilders.reindexAsync(index, filterQuery, options);
  }

  @Override
  public void reindexAll() {
    configure();
  }

  @Override
  public void upsertDocument(
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @Nonnull JsonNode document) {
    String indexName = _indexConvention.getTimeseriesAspectIndexName(entityName, aspectName);
    final UpdateRequest updateRequest =
        new UpdateRequest(indexName, docId)
            .detectNoop(false)
            .docAsUpsert(true)
            .doc(document.toString(), XContentType.JSON)
            .retryOnConflict(_numRetries);
    _bulkProcessor.add(updateRequest);
  }

  @Override
  public List<TimeseriesIndexSizeResult> getIndexSizes() {
    List<TimeseriesIndexSizeResult> res = new ArrayList<>();
    try {
      String indicesPattern = _indexConvention.getAllTimeseriesAspectIndicesPattern();
      Response r =
          _searchClient
              .getLowLevelClient()
              .performRequest(new Request("GET", "/" + indicesPattern + "/_stats"));
      JsonNode body = new ObjectMapper().readTree(r.getEntity().getContent());
      body.get("indices")
          .fields()
          .forEachRemaining(
              entry -> {
                TimeseriesIndexSizeResult elemResult = new TimeseriesIndexSizeResult();
                elemResult.setIndexName(entry.getKey());
                Optional<Pair<String, String>> indexEntityAndAspect =
                    _indexConvention.getEntityAndAspectName(entry.getKey());
                if (indexEntityAndAspect.isPresent()) {
                  elemResult.setEntityName(indexEntityAndAspect.get().getFirst());
                  elemResult.setAspectName(indexEntityAndAspect.get().getSecond());
                }
                int sizeBytes =
                    entry.getValue().get("primaries").get("store").get("size_in_bytes").asInt();
                float sizeMb = (float) sizeBytes / 1000;
                elemResult.setSizeMb(sizeMb);
                res.add(elemResult);
              });
      return res;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long countByFilter(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nullable final Filter filter) {
    final String indexName = _indexConvention.getTimeseriesAspectIndexName(entityName, aspectName);
    final BoolQueryBuilder filterQueryBuilder =
        QueryBuilders.boolQuery().must(ESUtils.buildFilterQuery(filter, true));
    CountRequest countRequest = new CountRequest();
    countRequest.query(filterQueryBuilder);
    countRequest.indices(indexName);
    try {
      CountResponse resp = _searchClient.count(countRequest, RequestOptions.DEFAULT);
      return resp.getCount();
    } catch (IOException e) {
      log.error("Count query failed:", e);
      throw new ESQueryException("Count query failed:", e);
    }
  }

  @Override
  public List<EnvelopedAspect> getAspectValues(
      @Nonnull final Urn urn,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nullable final Long startTimeMillis,
      @Nullable final Long endTimeMillis,
      @Nullable final Integer limit,
      @Nullable final Filter filter,
      @Nullable final SortCriterion sort) {
    final BoolQueryBuilder filterQueryBuilder =
        QueryBuilders.boolQuery().must(ESUtils.buildFilterQuery(filter, true));
    filterQueryBuilder.must(QueryBuilders.matchQuery("urn", urn.toString()));
    // NOTE: We are interested only in the un-exploded rows as only they carry the `event` payload.
    filterQueryBuilder.mustNot(QueryBuilders.termQuery(MappingsBuilder.IS_EXPLODED_FIELD, true));
    if (startTimeMillis != null) {
      Criterion startTimeCriterion =
          new Criterion()
              .setField(TIMESTAMP_FIELD)
              .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
              .setValue(startTimeMillis.toString());
      filterQueryBuilder.must(ESUtils.getQueryBuilderFromCriterion(startTimeCriterion, true));
    }
    if (endTimeMillis != null) {
      Criterion endTimeCriterion =
          new Criterion()
              .setField(TIMESTAMP_FIELD)
              .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
              .setValue(endTimeMillis.toString());
      filterQueryBuilder.must(ESUtils.getQueryBuilderFromCriterion(endTimeCriterion, true));
    }
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(filterQueryBuilder);
    searchSourceBuilder.size(limit != null ? limit : DEFAULT_LIMIT);

    if (sort != null) {
      final SortOrder esSortOrder =
          (sort.getOrder() == com.linkedin.metadata.query.filter.SortOrder.ASCENDING)
              ? SortOrder.ASC
              : SortOrder.DESC;
      searchSourceBuilder.sort(SortBuilders.fieldSort(sort.getField()).order(esSortOrder));
    } else {
      // By default, sort by the timestampMillis descending.
      searchSourceBuilder.sort(SortBuilders.fieldSort("@timestamp").order(SortOrder.DESC));
    }

    final SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder);

    String indexName = _indexConvention.getTimeseriesAspectIndexName(entityName, aspectName);
    searchRequest.indices(indexName);

    log.debug("Search request is: " + searchRequest);
    SearchHits hits;
    try (Timer.Context ignored =
        MetricUtils.timer(this.getClass(), "searchAspectValues_search").time()) {
      final SearchResponse searchResponse =
          _searchClient.search(searchRequest, RequestOptions.DEFAULT);
      hits = searchResponse.getHits();
    } catch (Exception e) {
      log.error("Search query failed:", e);
      throw new ESQueryException("Search query failed:", e);
    }
    return Arrays.stream(hits.getHits())
        .map(ElasticSearchTimeseriesAspectService::parseDocument)
        .collect(Collectors.toList());
  }

  @Override
  @Nonnull
  public GenericTable getAggregatedStats(
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull AggregationSpec[] aggregationSpecs,
      @Nullable Filter filter,
      @Nullable GroupingBucket[] groupingBuckets) {
    return _esAggregatedStatsDAO.getAggregatedStats(
        entityName, aspectName, aggregationSpecs, filter, groupingBuckets);
  }

  /**
   * A generic delete by filter API which uses elasticsearch's deleteByQuery. NOTE: There is no need
   * for the client to explicitly walk each scroll page with this approach. Elastic will
   * synchronously delete all of the documents matching the query that is specified by the filter,
   * and internally handles the batching logic by the scroll page size specified(i.e. the
   * DEFAULT_LIMIT value of 10,000).
   *
   * @param entityName the name of the entity.
   * @param aspectName the name of the aspect.
   * @param filter the filter to be used for deletion of the documents on the index.
   * @return the number of documents returned.
   */
  @Nonnull
  @Override
  public DeleteAspectValuesResult deleteAspectValues(
      @Nonnull String entityName, @Nonnull String aspectName, @Nonnull Filter filter) {
    final String indexName = _indexConvention.getTimeseriesAspectIndexName(entityName, aspectName);
    final BoolQueryBuilder filterQueryBuilder = ESUtils.buildFilterQuery(filter, true);

    final Optional<DeleteAspectValuesResult> result =
        _bulkProcessor
            .deleteByQuery(
                filterQueryBuilder, false, DEFAULT_LIMIT, TimeValue.timeValueMinutes(10), indexName)
            .map(
                response ->
                    new DeleteAspectValuesResult().setNumDocsDeleted(response.getDeleted()));

    if (result.isPresent()) {
      return result.get();
    } else {
      log.error("Delete query failed");
      throw new ESQueryException("Delete query failed");
    }
  }

  @Nonnull
  @Override
  public String deleteAspectValuesAsync(
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter,
      @Nonnull BatchWriteOperationsOptions options) {
    final String indexName = _indexConvention.getTimeseriesAspectIndexName(entityName, aspectName);
    final BoolQueryBuilder filterQueryBuilder = ESUtils.buildFilterQuery(filter, true);
    final int batchSize = options.getBatchSize() > 0 ? options.getBatchSize() : DEFAULT_LIMIT;
    TimeValue timeout =
        options.getTimeoutSeconds() > 0
            ? TimeValue.timeValueSeconds(options.getTimeoutSeconds())
            : null;
    final Optional<TaskSubmissionResponse> result =
        _bulkProcessor.deleteByQueryAsync(filterQueryBuilder, false, batchSize, timeout, indexName);

    if (result.isPresent()) {
      return result.get().getTask();
    } else {
      log.error("Async delete query failed");
      throw new ESQueryException("Async delete query failed");
    }
  }

  @Override
  public String reindexAsync(
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter,
      @Nonnull BatchWriteOperationsOptions options) {
    final String indexName = _indexConvention.getTimeseriesAspectIndexName(entityName, aspectName);
    final BoolQueryBuilder filterQueryBuilder = ESUtils.buildFilterQuery(filter, true);
    try {
      return this.reindexAsync(indexName, filterQueryBuilder, options);
    } catch (Exception e) {
      log.error("Async reindex failed");
      throw new ESQueryException("Async reindex failed", e);
    }
  }

  @Nonnull
  @Override
  public DeleteAspectValuesResult rollbackTimeseriesAspects(@Nonnull String runId) {
    DeleteAspectValuesResult rollbackResult = new DeleteAspectValuesResult();
    // Construct the runId filter for deletion.
    Filter filter = QueryUtils.newFilter("runId", runId);

    // Delete the timeseries aspects across all entities with the runId.
    for (Map.Entry<String, EntitySpec> entry : _entityRegistry.getEntitySpecs().entrySet()) {
      for (AspectSpec aspectSpec : entry.getValue().getAspectSpecs()) {
        if (aspectSpec.isTimeseries()) {
          DeleteAspectValuesResult result =
              this.deleteAspectValues(entry.getKey(), aspectSpec.getName(), filter);
          rollbackResult.setNumDocsDeleted(
              rollbackResult.getNumDocsDeleted() + result.getNumDocsDeleted());
          log.info(
              "Number of timeseries docs deleted for entity:{}, aspect:{}, runId:{}={}",
              entry.getKey(),
              aspectSpec.getName(),
              runId,
              result.getNumDocsDeleted());
        }
      }
    }

    return rollbackResult;
  }
}
