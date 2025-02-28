package com.linkedin.datahub.upgrade.nocodecleanup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;

// Do we need SQL-tech specific migration paths?
@RequiredArgsConstructor
public class DeleteLegacySearchIndicesStep implements UpgradeStep {

  private final String deletePattern;

  private final RestHighLevelClient _searchClient;

  public DeleteLegacySearchIndicesStep(
      final RestHighLevelClient searchClient, final IndexConvention indexConvention) {
    _searchClient = searchClient;
    deletePattern = indexConvention.getPrefix().map(p -> p + "_").orElse("") + "*document*";
  }

  @Override
  public String id() {
    return "DeleteLegacySearchIndicesStep";
  }

  @Override
  public int retryCount() {
    return 1;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      DeleteIndexRequest request = new DeleteIndexRequest(deletePattern);
      try {
        _searchClient.indices().delete(request, RequestOptions.DEFAULT);
      } catch (Exception e) {
        context.report().addLine("Failed to delete legacy search index: %s", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
