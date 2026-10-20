package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.GlossaryNodeChildrenCount;
import com.linkedin.datahub.graphql.resolvers.load.GlossaryNodeChildrenCountBatchLoader;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;

public class GlossaryNodeChildrenCountResolver
    implements DataFetcher<CompletableFuture<GlossaryNodeChildrenCount>> {

  @Override
  public CompletableFuture<GlossaryNodeChildrenCount> get(final DataFetchingEnvironment environment)
      throws Exception {
    final String urn = ((Entity) environment.getSource()).getUrn();
    // Fail fast rather than letting a malformed URN into a shared batch filter.
    final Urn sourceUrn = Urn.createFromString(urn);
    final QueryContext context = environment.getContext();
    if (!canView(context.getOperationContext(), sourceUrn)) {
      final GlossaryNodeChildrenCount result = new GlossaryNodeChildrenCount();
      result.setTermsCount(0);
      result.setNodesCount(0);
      return CompletableFuture.completedFuture(result);
    }

    final DataLoader<String, GlossaryNodeChildrenCount> loader =
        environment
            .getDataLoaderRegistry()
            .getDataLoader(GlossaryNodeChildrenCountBatchLoader.LOADER_NAME);
    return loader.load(urn);
  }
}
