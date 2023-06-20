package com.linkedin.datahub.graphql.resolvers.operations;

import com.linkedin.datahub.graphql.generated.GetIndexSizesResult;
import com.linkedin.datahub.graphql.generated.IndexSizeResult;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class GetIndexSizesResolver implements DataFetcher<CompletableFuture<GetIndexSizesResult>> {

  private final TimeseriesAspectService _timeseriesAspectService;

  @Override
  public CompletableFuture<GetIndexSizesResult> get(DataFetchingEnvironment environment) throws Exception {
    return CompletableFuture.supplyAsync(() -> {
      // TODO(indy): Implement listing specific indices using indices input parameter
      List<TimeseriesIndexSizeResult> res = _timeseriesAspectService.getIndexSizes();
      return mapResult(res);
    });
  }

  private GetIndexSizesResult mapResult(List<TimeseriesIndexSizeResult> indexSizeResults) {
    return new GetIndexSizesResult(
        indexSizeResults.stream().map(result -> new IndexSizeResult(
                result.getIndexName(), result.getEntityName(), result.getAspectName(), result.getSizeMb())
            ).collect(Collectors.toList()));
  }
}