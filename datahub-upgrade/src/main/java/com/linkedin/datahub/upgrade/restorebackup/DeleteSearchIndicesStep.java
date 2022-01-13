package com.linkedin.datahub.upgrade.restorebackup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;


public class DeleteSearchIndicesStep implements UpgradeStep {

  private final String _deletePattern;

  private final RestHighLevelClient _searchClient;

  public DeleteSearchIndicesStep(final RestHighLevelClient searchClient) {
    _searchClient = searchClient;
    String prefix = System.getenv("INDEX_PREFIX");
    _deletePattern = Optional.ofNullable(prefix).map(p -> p + "_").orElse("") + "*index_v2*";
  }

  @Override
  public String id() {
    return "DeleteSearchIndicesStep";
  }

  @Override
  public int retryCount() {
    return 1;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      DeleteIndexRequest request = new DeleteIndexRequest(_deletePattern);
      String[] indices = getIndices(context);
      DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(indices).setQuery(QueryBuilders.matchAllQuery());
      try {
        _searchClient.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);
      } catch (Exception e) {
        context.report().addLine(String.format("Failed to delete content of search indices: %s", e.toString()));
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private String[] getIndices(UpgradeContext context) {
    try {
      GetIndexResponse response =
          _searchClient.indices().get(new GetIndexRequest(_deletePattern), RequestOptions.DEFAULT);
      return response.getIndices();
    } catch (IOException e) {
      context.report().addLine(String.format("Failed to fetch indices matching pattern %s", _deletePattern));
      return new String[]{};
    }
  }
}
