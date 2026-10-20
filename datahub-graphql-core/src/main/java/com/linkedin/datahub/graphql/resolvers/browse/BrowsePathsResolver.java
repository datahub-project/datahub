package com.linkedin.datahub.graphql.resolvers.browse;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowsePathsInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrowsePathsResolver implements DataFetcher<CompletableFuture<List<BrowsePath>>> {

  private static final Logger _logger =
      LoggerFactory.getLogger(BrowsePathsResolver.class.getName());

  private final Map<EntityType, BrowsableEntityType<?, ?>> _typeToEntity;

  public BrowsePathsResolver(@Nonnull final List<BrowsableEntityType<?, ?>> browsableEntities) {
    _typeToEntity =
        browsableEntities.stream()
            .collect(Collectors.toMap(BrowsableEntityType::type, entity -> entity));
  }

  @Override
  public CompletableFuture<List<BrowsePath>> get(DataFetchingEnvironment environment) {
    final BrowsePathsInput input =
        bindArgument(environment.getArgument("input"), BrowsePathsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            _logger.debug(
                "Fetch browse paths. entity type: {}, urn: {}", input.getType(), input.getUrn());
            final QueryContext context = environment.getContext();
            if (_typeToEntity.containsKey(input.getType())) {
              if (isHidden(context, input)) {
                return Collections.emptyList();
              }
              return _typeToEntity.get(input.getType()).browsePaths(input.getUrn(), context);
            }
            // Browse path is impl detail.
            return Collections.emptyList();
          } catch (Exception e) {
            _logger.error(
                "Failed to retrieve browse paths: "
                    + String.format("entity type %s, urn %s", input.getType(), input.getUrn())
                    + " "
                    + e.getMessage());
            throw new RuntimeException(
                "Failed to retrieve browse paths: "
                    + String.format("entity type %s, urn %s", input.getType(), input.getUrn()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * An unviewable urn of the requested type gets the empty answer a missing one gets; any other urn
   * goes through the type's own parser so it fails the same way for hidden and missing.
   */
  private static boolean isHidden(
      @Nonnull final QueryContext context, @Nonnull final BrowsePathsInput input) {
    final Urn urn;
    try {
      urn = Urn.createFromString(input.getUrn());
    } catch (URISyntaxException e) {
      return false;
    }
    return EntityTypeMapper.getName(input.getType()).equals(urn.getEntityType())
        && !canView(context.getOperationContext(), urn);
  }
}
