package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Wraps a batch resolver for a {@code [Entity!]} list so that, while enum values a newer version
 * added are tolerated, entries that resolved to null are dropped: an entity holding such a value in
 * a required field maps to null rather than failing the list (see e.g. {@code
 * DataHubViewMapper#mapIfRepresentable}), as does one that no longer exists. Otherwise a null fails
 * the list, as before.
 *
 * <p>The loader returns a mapper's null as a {@link DataFetcherResult} without data, so those are
 * dropped too. One that carries errors is kept, so the errors still surface.
 */
public final class OmitUnrepresentableEntities {

  private OmitUnrepresentableEntities() {}

  @Nonnull
  public static <T> DataFetcher<CompletableFuture<List<T>>> from(
      @Nonnull final LoadableTypeBatchResolver<T, ?> resolver) {
    return env -> {
      final CompletableFuture<List<T>> loaded = resolver.get(env);
      if (loaded == null || !handling().toleratesUnknownEnum()) {
        return loaded;
      }
      return loaded.thenApply(
          entities ->
              entities.stream()
                  .filter(
                      entity ->
                          entity != null
                              && !(entity instanceof DataFetcherResult<?> result
                                  && result.getData() == null
                                  && result.getErrors().isEmpty()))
                  .collect(Collectors.toList()));
    };
  }
}
