package com.linkedin.metadata.elasticsearch.update;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import org.elasticsearch.client.RestHighLevelClient;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

public class ESBulkProcessorTest {

    @Test
    public void testESBulkProcessorBuilder() {
        RestHighLevelClient mock = Mockito.mock(RestHighLevelClient.class);
        ESBulkProcessor test = ESBulkProcessor.builder(mock).build();
        assertNotNull(test);
    }
}
