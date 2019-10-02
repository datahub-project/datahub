package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.events.metadata.ChangeType;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import java.util.List;

@Slf4j
public class ElasticsearchConnector {

  private RestClient _restClient;
  private RestHighLevelClient _client;

  private Integer _esPort;
  private String[] _esHosts;
  private Integer _threadCount;

  private BulkProcessor _bulkProcessor;
  private Integer _bulkRequestsLimit;
  private Integer _bulkFlushPeriod;
  private static final int DEFAULT_NUMBER_OF_RETRIES = 3; // TODO: Test and also add these into config
  private static final long DEFAULT_RETRY_INTERVAL = 1L;

  public ElasticsearchConnector(List<String> hosts, Integer port, Integer threadCount, Integer bulkRequestsLimit,
      Integer bulkFlushPeriod) {

    _esPort = port;
    _esHosts = hosts.toArray(new String[0]);
    _threadCount = threadCount;
    _bulkRequestsLimit = bulkRequestsLimit;
    _bulkFlushPeriod = bulkFlushPeriod;

    initClient();
    initBulkProcessor();
  }

  private void initClient() {
    try {
      _restClient = loadRestHttpClient(_esHosts, _esPort, _threadCount);
      _client = new RestHighLevelClient(_restClient);
    } catch (Exception ex) {
      log.error("Error: RestClient is not properly initialized. " + ex.toString());
    }
  }

  private void initBulkProcessor() {
    BulkProcessor.Listener listener = new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {

      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        log.info("Successfully feeded bulk request. Number of events: " + response.getItems().length + " Took time ms: "
            + response.getTookInMillis());
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        log.info("Error feeding bulk request. No retries left", failure);
      }
    };

    ThreadPool threadPool = new ThreadPool(Settings.builder().put(Settings.EMPTY).build());
    _bulkProcessor =
        new BulkProcessor.Builder(_client::bulkAsync, listener, threadPool).setBulkActions(_bulkRequestsLimit)
            .setFlushInterval(TimeValue.timeValueSeconds(_bulkFlushPeriod))
            .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(DEFAULT_RETRY_INTERVAL),
                DEFAULT_NUMBER_OF_RETRIES))
            .build();
  }

  public void feedElasticEvent(@Nonnull ElasticEvent event) {
    if (event.getActionType().equals(ChangeType.DELETE)) {
      _bulkProcessor.add(createDeleteRequest(event));
    } else if (event.getActionType().equals(ChangeType.CREATE)) {
      _bulkProcessor.add(createIndexRequest(event));
    } else if (event.getActionType().equals(ChangeType.UPDATE)) {
      _bulkProcessor.add(createUpsertRequest(event));
    }
  }

  @Nonnull
  private static IndexRequest createIndexRequest(@Nonnull ElasticEvent event) {
    return new IndexRequest(event.getIndex(), event.getType(), event.getId()).source(event.buildJson());
  }

  @Nonnull
  private static DeleteRequest createDeleteRequest(@Nonnull ElasticEvent event) {
    return new DeleteRequest(event.getIndex(), event.getType(), event.getId());
  }

  @Nonnull
  private static UpdateRequest createUpsertRequest(@Nonnull ElasticEvent event) {
    IndexRequest indexRequest = new IndexRequest(event.getIndex(), event.getType(), event.getId()).source(event.buildJson());
    return new UpdateRequest(event.getIndex(), event.getType(),
        event.getId()).doc(event.buildJson()).detectNoop(false).upsert(indexRequest).retryOnConflict(3);
  }

  @Nonnull
  private static RestClient loadRestHttpClient(String[] hosts, Integer port, int threadCount) {

    HttpHost[] httpHosts = new HttpHost[hosts.length];
    for (int h = 0; h < hosts.length; h++) {
      httpHosts[h] = new HttpHost(hosts[h], port, "http");
    }

    RestClientBuilder builder = RestClient.builder(httpHosts).setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
        // Configure number of threads for clients
        .setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(threadCount).build()));

    // TODO: Configure timeouts
    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(0));

    return builder.build();
  }
}