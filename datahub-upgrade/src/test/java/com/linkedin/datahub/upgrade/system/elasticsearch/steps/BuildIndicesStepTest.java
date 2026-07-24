package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeReport;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;
import com.linkedin.metadata.utils.elasticsearch.TaskFailureDetail;
import com.linkedin.metadata.utils.elasticsearch.TaskFailureParseResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BuildIndicesStepTest {

  private BuildIndicesStep step;
  private UpgradeContext upgradeContext;
  private DefaultUpgradeReport upgradeReport;

  @BeforeMethod
  public void setup() {
    step = new BuildIndicesStep(Collections.emptyList(), Collections.emptySet());
    upgradeContext = mock(UpgradeContext.class);
    upgradeReport = new DefaultUpgradeReport();
    when(upgradeContext.report()).thenReturn(upgradeReport);
  }

  @Test
  public void testFinishParallelReindex_enrichesReportWithDocumentFailures() {
    Map<String, ReindexResult> results =
        Map.of("dataset_v2", ReindexResult.FAILED_DOC_COUNT_MISMATCH);
    TaskFailureParseResult documentFailures =
        new TaskFailureParseResult(
            List.of(
                new TaskFailureDetail(
                    "my_index",
                    "doc1",
                    "mapper_parsing_exception",
                    "failed to parse field [foo]",
                    2)),
            1);
    Map<String, TaskFailureParseResult> failuresByIndex = Map.of("dataset_v2", documentFailures);

    UpgradeStepResult result = step.finishParallelReindex(upgradeContext, results, failuresByIndex);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    String reportText = String.join("\n", upgradeReport.lines());
    assertTrue(reportText.contains("BuildIndicesStep failed"));
    assertTrue(reportText.contains("dataset_v2"));
    assertTrue(
        reportText.contains("mapper_parsing_exception")
            || reportText.contains("documentFailures="));
  }

  @Test
  public void testFinishParallelReindex_successWithEmptyFailures() {
    Map<String, ReindexResult> results = Map.of("dataset_v2", ReindexResult.REINDEXED);

    UpgradeStepResult result = step.finishParallelReindex(upgradeContext, results, Map.of());

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertTrue(upgradeReport.lines().isEmpty());
  }
}
