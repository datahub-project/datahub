package com.linkedin.metadata.systemmetadata;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.config.SystemMetadataServiceConfig;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.SearchTestUtils;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.update.UpdateRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESSystemMetadataDAOTest {

  private static final IndexConvention TEST_INDEX_CONVENTION =
      IndexConventionImpl.noPrefix("md5", SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION);

  private ESBulkProcessor mockBulkProcessor;
  private ESSystemMetadataDAO dao;

  @BeforeMethod
  public void setUp() {
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    mockBulkProcessor = mock(ESBulkProcessor.class);
    SystemMetadataServiceConfig config = mock(SystemMetadataServiceConfig.class);
    dao = new ESSystemMetadataDAO(mockClient, TEST_INDEX_CONVENTION, mockBulkProcessor, 0, config);
  }

  @Test
  public void testUpsertDocumentRoutesByDocId() {
    String docId = "sysmeta-doc-abc";
    String document = "{\"urn\":\"urn:li:dataset:foo\",\"aspect\":\"ownership\",\"removed\":false}";

    dao.upsertDocument(docId, document);

    ArgumentCaptor<UpdateRequest> captor = ArgumentCaptor.forClass(UpdateRequest.class);
    verify(mockBulkProcessor).add(eq(docId), captor.capture());
    UpdateRequest captured = captor.getValue();
    assertNotNull(captured);
    assertEquals(captured.id(), docId);
  }

  @Test
  public void testUpsertDocumentUsesRoutingOverload() {
    dao.upsertDocument("doc-1", "{}");
    verify(mockBulkProcessor).add(any(String.class), any(UpdateRequest.class));
  }
}
