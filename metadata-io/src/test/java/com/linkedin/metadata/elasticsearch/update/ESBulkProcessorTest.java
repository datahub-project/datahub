package com.linkedin.metadata.elasticsearch.update;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import org.mockito.Mockito;
import org.opensearch.client.RestHighLevelClient;
import org.testng.annotations.Test;

public class ESBulkProcessorTest {

  @Test
  public void testESBulkProcessorBuilder() {
    RestHighLevelClient mock = Mockito.mock(RestHighLevelClient.class);
    ESBulkProcessor test = ESBulkProcessor.builder(mock).build();
    assertNotNull(test);
  }
}
