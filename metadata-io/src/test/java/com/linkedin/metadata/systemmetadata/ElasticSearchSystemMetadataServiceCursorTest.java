package com.linkedin.metadata.systemmetadata;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.SystemMetadataServiceConfig;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for the run-id cursor read, with the search DAO mocked. */
public class ElasticSearchSystemMetadataServiceCursorTest {

  private static final String RUN_ID = "test-run-id";
  private static final int PAGE_SIZE = 5;

  private ESSystemMetadataDAO mockDao;
  private ElasticSearchSystemMetadataService service;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    mockDao = mock(ESSystemMetadataDAO.class);
    service =
        new ElasticSearchSystemMetadataService(
            mock(ESBulkProcessor.class),
            mock(IndexConvention.class),
            mockDao,
            mock(ESIndexBuilder.class),
            "md5",
            mock(SystemMetadataServiceConfig.class));
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testFindByRunIdAfter_ThrowsWhenScrollFails() {
    // The DAO logs a search failure and returns null; that must not read as "no more rows".
    when(mockDao.scroll(any(), any(), eq(true), any(), isNull(), isNull(), eq(PAGE_SIZE)))
        .thenReturn(null);

    assertThrows(
        IllegalStateException.class,
        () -> service.findByRunIdAfter(opContext, RUN_ID, true, null, PAGE_SIZE));
  }

  @Test
  public void testFindByRunIdAfter_ReturnsEmptyListWhenNoRowsMatch() {
    // Built before when(...): creating and stubbing mocks inside thenReturn is unfinished stubbing.
    SearchResponse noHits = responseWithNoHits();
    when(mockDao.scroll(any(), any(), eq(true), any(), isNull(), isNull(), eq(PAGE_SIZE)))
        .thenReturn(noHits);

    List<AspectRowSummary> rows =
        service.findByRunIdAfter(opContext, RUN_ID, true, null, PAGE_SIZE);

    assertTrue(rows.isEmpty());
  }

  @Test
  public void testFindByRunIdAfter_FirstPageStartsWithoutCursor() {
    // Built before when(...): creating and stubbing mocks inside thenReturn is unfinished stubbing.
    SearchResponse noHits = responseWithNoHits();
    when(mockDao.scroll(any(), any(), eq(true), any(), isNull(), isNull(), eq(PAGE_SIZE)))
        .thenReturn(noHits);

    service.findByRunIdAfter(opContext, RUN_ID, true, null, PAGE_SIZE);

    ArgumentCaptor<String> scrollId = ArgumentCaptor.forClass(String.class);
    verify(mockDao)
        .scroll(
            any(OperationContext.class),
            any(BoolQueryBuilder.class),
            eq(true),
            scrollId.capture(),
            isNull(),
            isNull(),
            eq(PAGE_SIZE));
    assertNull(scrollId.getValue());
  }

  @Test
  public void testFindByRunIdAfter_ContinuesAfterTheGivenRow() {
    // Built before when(...): creating and stubbing mocks inside thenReturn is unfinished stubbing.
    SearchResponse noHits = responseWithNoHits();
    when(mockDao.scroll(any(), any(), eq(true), any(), isNull(), isNull(), eq(PAGE_SIZE)))
        .thenReturn(noHits);
    AspectRowSummary after = new AspectRowSummary();
    after.setUrn("urn:li:dataset:(urn:li:dataPlatform:hive,after-dataset,PROD)");
    after.setAspectName("datasetProperties");

    service.findByRunIdAfter(opContext, RUN_ID, true, after, PAGE_SIZE);

    ArgumentCaptor<String> scrollId = ArgumentCaptor.forClass(String.class);
    verify(mockDao)
        .scroll(
            any(OperationContext.class),
            any(BoolQueryBuilder.class),
            eq(true),
            scrollId.capture(),
            isNull(),
            isNull(),
            eq(PAGE_SIZE));
    assertNotNull(scrollId.getValue());
    // scroll sorts by (urn, aspect), so the cursor is that pair of the last row read.
    assertEquals(
        SearchAfterWrapper.fromScrollId(scrollId.getValue()).getSort(),
        new Object[] {after.getUrn(), after.getAspectName()});
  }

  private static SearchResponse responseWithNoHits() {
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits())
        .thenReturn(
            new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0f));
    return response;
  }
}
