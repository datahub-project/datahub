package com.linkedin.metadata.kafka.elasticsearch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ElasticsearchConnectorTest {

  private ESBulkProcessor mockBulkProcessor;
  private ElasticsearchConnector connector;

  @BeforeMethod
  public void setUp() {
    mockBulkProcessor = mock(ESBulkProcessor.class);
    connector = new ElasticsearchConnector(mockBulkProcessor, 3);
  }

  private static JsonElasticEvent makeEvent(String id, ChangeType changeType, String document) {
    JsonElasticEvent event = new JsonElasticEvent(document);
    event.setId(id);
    event.setIndex("test_index");
    event.setActionType(changeType);
    return event;
  }

  @Test
  public void testCreateRoutesByEventId() {
    JsonElasticEvent event = makeEvent("create-id-1", ChangeType.CREATE, "{\"field\":\"value\"}");

    connector.feedElasticEvent(event);

    verify(mockBulkProcessor).add(eq("create-id-1"), any(IndexRequest.class));
  }

  @Test
  public void testCreateEntityRoutesByEventId() {
    JsonElasticEvent event =
        makeEvent("create-entity-id", ChangeType.CREATE_ENTITY, "{\"field\":\"value\"}");

    connector.feedElasticEvent(event);

    verify(mockBulkProcessor).add(eq("create-entity-id"), any(IndexRequest.class));
  }

  @Test
  public void testUpdateRoutesByEventId() {
    JsonElasticEvent event = makeEvent("update-id-2", ChangeType.UPDATE, "{\"field\":\"value\"}");

    connector.feedElasticEvent(event);

    verify(mockBulkProcessor).add(eq("update-id-2"), any(UpdateRequest.class));
  }

  @Test
  public void testDeleteRoutesByEventId() {
    JsonElasticEvent event = makeEvent("delete-id-3", ChangeType.DELETE, "{}");

    connector.feedElasticEvent(event);

    verify(mockBulkProcessor).add(eq("delete-id-3"), any(DeleteRequest.class));
  }

  @Test
  public void testUnhandledChangeTypeDoesNotWrite() {
    JsonElasticEvent event = makeEvent("noop-id", ChangeType.RESTATE, "{}");

    connector.feedElasticEvent(event);

    verifyNoInteractions(mockBulkProcessor);
  }
}
