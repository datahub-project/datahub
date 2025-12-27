package com.linkedin.datahub.graphql.resolvers.organization;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.dataloader.DataLoader;

/**
 * GraphQL resolver for the organization query that adds authorization checks. Ensures users can
 * only view organizations they have access to.
 */
public class OrganizationResolver<T, K> implements DataFetcher<CompletableFuture<T>> {

  private final LoadableType<T, K> _loadableType;
  private final Function<DataFetchingEnvironment, K> _keyProvider;

  public OrganizationResolver(
      final LoadableType<T, K> loadableType,
      final Function<DataFetchingEnvironment, K> keyProvider) {
    _loadableType = loadableType;
    _keyProvider = keyProvider;
  }

  @Override
  public CompletableFuture<T> get(DataFetchingEnvironment environment) {
    final K key = _keyProvider.apply(environment);
    if (key == null) {
      return CompletableFuture.completedFuture(null);
    }

    @SuppressWarnings("deprecation")
    final QueryContext context = environment.getContext();

    // Check authorization before loading
    try {
      final Urn organizationUrn = Urn.createFromString(key.toString());
      if (!OrganizationAuthUtils.canViewOrganization(context, organizationUrn)) {
        throw new AuthorizationException("Unauthorized to view organization " + organizationUrn);
      }
    } catch (URISyntaxException e) {
      // If URN parsing fails, let the loader handle it
    } catch (AuthorizationException e) {
      throw e;
    }

    // Load the organization using the standard loader
    final DataLoader<K, T> loader =
        environment.getDataLoaderRegistry().getDataLoader(_loadableType.name());
    return loader.load(key);
  }
}
