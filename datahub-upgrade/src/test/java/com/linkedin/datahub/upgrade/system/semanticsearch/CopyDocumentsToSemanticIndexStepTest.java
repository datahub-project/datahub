package com.linkedin.datahub.upgrade.system.semanticsearch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.function.Function;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.index.reindex.ReindexRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CopyDocumentsToSemanticIndexStepTest {

  @Mock private SearchClientShim<?> searchClient;

  @Mock private EntityService<?> entityService;

  @Mock private IndexConvention indexConvention;

  @Mock private UpgradeContext upgradeContext;

  private OperationContext opContext;

  private CopyDocumentsToSemanticIndexStep step;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    opContext = TestOperationContexts.systemContextNoValidate();
    when(upgradeContext.opContext()).thenReturn(opContext);
  }

  @Test
  public void testExecutable_Success() throws Exception {
    String entityName = "dataset";
    String baseIndexName = "datasetindex_v2";
    String semanticIndexName = "datasetindex_v2_semantic_search";
    String taskId = "task-12345";

    when(indexConvention.getEntityIndexName(entityName)).thenReturn(baseIndexName);
    when(indexConvention.getEntityIndexNameSemantic(entityName)).thenReturn(semanticIndexName);
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    when(searchClient.submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class)))
        .thenReturn(taskId);

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    assertEquals(result.stepId(), "CopyDocumentsToSemanticIndex_dataset");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(searchClient).indexExists(any(GetIndexRequest.class), any(RequestOptions.class));
    verify(searchClient).submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class));
  }
}
