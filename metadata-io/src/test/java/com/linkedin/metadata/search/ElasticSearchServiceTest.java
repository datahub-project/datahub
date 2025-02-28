package com.linkedin.metadata.search;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ElasticSearchServiceTest {
  private static final Urn TEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)");
  private static final String TEST_DOC_ID =
      URLEncoder.encode(TEST_URN.toString(), StandardCharsets.UTF_8);
  private static final int MAX_RUN_IDS_INDEXED = 25;

  @Mock private ESWriteDAO mockEsWriteDAO;

  private ElasticSearchService testInstance;
  private static final OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(
            mock(ESIndexBuilder.class),
            opContext.getEntityRegistry(),
            opContext.getSearchContext().getIndexConvention(),
            mock(SettingsBuilder.class));
    testInstance =
        new ElasticSearchService(
            indexBuilders, mock(ESSearchDAO.class), mock(ESBrowseDAO.class), mockEsWriteDAO);
  }

  @Test
  public void testAppendRunId_ValidRunId() {
    String runId = "test-run-id";

    // Execute
    testInstance.appendRunId(opContext, TEST_URN, runId);

    // Capture and verify the script update parameters
    ArgumentCaptor<Map<String, Object>> upsertCaptor = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<String> scriptCaptor = ArgumentCaptor.forClass(String.class);

    verify(mockEsWriteDAO)
        .applyScriptUpdate(
            eq(opContext),
            eq(TEST_URN.getEntityType()),
            eq(TEST_DOC_ID),
            scriptCaptor.capture(),
            upsertCaptor.capture());

    // Verify script content
    String expectedScript =
        String.format(
            "if (ctx._source.containsKey('runId')) { "
                + "if (!ctx._source.runId.contains('%s')) { "
                + "ctx._source.runId.add('%s'); "
                + "if (ctx._source.runId.length > %s) { ctx._source.runId.remove(0) } } "
                + "} else { ctx._source.runId = ['%s'] }",
            runId, runId, MAX_RUN_IDS_INDEXED, runId);
    assertEquals(scriptCaptor.getValue(), expectedScript);

    // Verify upsert document
    Map<String, Object> capturedUpsert = upsertCaptor.getValue();
    assertEquals(capturedUpsert.get("runId"), Collections.singletonList(runId));
  }

  @Test
  public void testAppendRunId_NullRunId() {
    // Execute with null runId
    testInstance.appendRunId(opContext, TEST_URN, null);

    // Verify the script update is still called with null handling
    ArgumentCaptor<Map<String, Object>> upsertCaptor = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<String> scriptCaptor = ArgumentCaptor.forClass(String.class);

    verify(mockEsWriteDAO)
        .applyScriptUpdate(
            eq(opContext),
            eq(TEST_URN.getEntityType()),
            eq(TEST_DOC_ID),
            scriptCaptor.capture(),
            upsertCaptor.capture());

    // Verify script content handles null
    String expectedScript =
        String.format(
            "if (ctx._source.containsKey('runId')) { "
                + "if (!ctx._source.runId.contains('%s')) { "
                + "ctx._source.runId.add('%s'); "
                + "if (ctx._source.runId.length > %s) { ctx._source.runId.remove(0) } } "
                + "} else { ctx._source.runId = ['%s'] }",
            null, null, MAX_RUN_IDS_INDEXED, null);
    assertEquals(scriptCaptor.getValue(), expectedScript);

    Map<String, Object> capturedUpsert = upsertCaptor.getValue();
    assertEquals(capturedUpsert.get("runId"), Collections.singletonList(null));
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRunId_NullUrn() {
    testInstance.appendRunId(opContext, null, "test-run-id");
  }
}
