package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BulkEntitySemanticModelsInput;
import com.linkedin.datahub.graphql.generated.BulkEntitySemanticModelsResult;
import com.linkedin.datahub.graphql.generated.EntitySemanticModel;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SemanticModel;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * Resolves the SemanticModel that contains each of a set of member entities (metrics / logical
 * datasets), via a single scroll over the {@code Contains} graph relationship. Membership is stored
 * only on {@code semanticModelInfo.datasets} / {@code .metrics}; reverse lookup uses the graph
 * index (mirroring {@code BulkEntityDataProductsResolver} for DataProducts).
 *
 * <p>Also exposes a DataLoader used by {@code Metric.semanticModel} and {@code
 * Dataset.semanticModel} field resolvers so list pages batch the membership lookup.
 */
@Slf4j
public class BulkEntitySemanticModelsResolver
    implements DataFetcher<CompletableFuture<BulkEntitySemanticModelsResult>> {

  public static final String LOADER_NAME = "EntitySemanticModel";

  private static final String CONTAINS_RELATIONSHIP = "Contains";
  private static final int MAX_URNS = 100;
  private static final int RELATIONSHIP_SCROLL_COUNT = 1000;

  public BulkEntitySemanticModelsResolver() {}

  @Override
  public CompletableFuture<BulkEntitySemanticModelsResult> get(
      DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final OperationContext opContext = context.getOperationContext();
    final BulkEntitySemanticModelsInput input =
        bindArgument(environment.getArgument("input"), BulkEntitySemanticModelsInput.class);

    final List<String> inputUrns = input.getUrns();
    if (inputUrns.size() > MAX_URNS) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot fetch semantic models for more than %s entities at once", MAX_URNS));
    }
    // Parse up front so a malformed urn fails fast, synchronously, as a client error.
    inputUrns.forEach(UrnUtils::getUrn);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final Map<String, String> semanticModelByEntity =
              resolveSemanticModelByEntity(opContext, inputUrns);

          final List<EntitySemanticModel> entities =
              inputUrns.stream()
                  .map(
                      urn -> {
                        final EntitySemanticModel row = new EntitySemanticModel();
                        row.setUrn(urn);
                        final String semanticModelUrn = semanticModelByEntity.get(urn);
                        if (semanticModelUrn != null) {
                          final SemanticModel stub = new SemanticModel();
                          stub.setUrn(semanticModelUrn);
                          stub.setType(EntityType.SEMANTIC_MODEL);
                          row.setSemanticModel(stub);
                        }
                        return row;
                      })
                  .collect(Collectors.toList());

          final BulkEntitySemanticModelsResult result = new BulkEntitySemanticModelsResult();
          result.setEntities(entities);
          return result;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Maps each member entity urn to the urn of the SemanticModel that contains it (if any). When an
   * entity appears in more than one SemanticModel, the first viewable edge wins.
   */
  @Nonnull
  public static Map<String, String> resolveSemanticModelByEntity(
      @Nonnull final OperationContext opContext, @Nonnull final List<String> entityUrns) {
    final Map<String, String> result = new HashMap<>();
    if (entityUrns.isEmpty()) {
      return result;
    }

    final GraphRetriever graphRetriever = opContext.getRetrieverContext().getGraphRetriever();
    final Map<String, Boolean> viewableBySemanticModel = new HashMap<>();
    String scrollId = null;
    do {
      final RelatedEntitiesScrollResult scroll =
          graphRetriever.scrollRelatedEntities(
              null,
              QueryUtils.newFilter(QueryUtils.newCriterion("urn", new ArrayList<>(entityUrns))),
              null,
              EMPTY_FILTER,
              Set.of(CONTAINS_RELATIONSHIP),
              QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
              Collections.emptyList(),
              scrollId,
              RELATIONSHIP_SCROLL_COUNT,
              null,
              null);

      for (RelatedEntities related : scroll.getEntities()) {
        // Incoming Contains: source is the semantic model, destination is the member.
        final String semanticModelUrn = related.getSourceUrn();
        if (!Constants.SEMANTIC_MODEL_ENTITY_NAME.equals(
            UrnUtils.getUrn(semanticModelUrn).getEntityType())) {
          // Contains is also used by other containers; ignore non-semanticModel sources.
          continue;
        }
        final boolean viewable =
            viewableBySemanticModel.computeIfAbsent(
                semanticModelUrn, urn -> canView(opContext, UrnUtils.getUrn(urn)));
        if (viewable) {
          result.putIfAbsent(related.getDestinationUrn(), semanticModelUrn);
        }
      }

      scrollId = scroll.getEntities().isEmpty() ? null : scroll.getScrollId();
    } while (scrollId != null);

    return result;
  }

  /**
   * DataLoader that maps member entity urn → owning SemanticModel urn (or null). Used by {@code
   * Metric.semanticModel} / {@code Dataset.semanticModel} field resolvers.
   */
  public static DataLoader<String, String> createDataLoader(final QueryContext queryContext) {
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);
    return DataLoader.newDataLoader(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> {
                  final QueryContext context = (QueryContext) env.getContext();
                  final Map<String, String> resolved =
                      resolveSemanticModelByEntity(context.getOperationContext(), keys);
                  return keys.stream()
                      .map(urn -> resolved.getOrDefault(urn, null))
                      .collect(Collectors.toList());
                },
                LOADER_NAME,
                "batchLoad"),
        options);
  }

  /**
   * Field DataFetcher for {@code Metric.semanticModel} and {@code Dataset.semanticModel}: looks up
   * the owning SemanticModel urn via {@link #LOADER_NAME}, then hydrates via the SemanticModel
   * entity DataLoader.
   */
  public static DataFetcher<CompletableFuture<SemanticModel>> fieldResolver(
      @Nonnull final String semanticModelTypeName) {
    return environment -> {
      final Object source = environment.getSource();
      final String entityUrn = extractUrn(source);
      if (entityUrn == null) {
        return CompletableFuture.completedFuture(null);
      }
      final DataLoader<String, String> membershipLoader =
          environment.getDataLoaderRegistry().getDataLoader(LOADER_NAME);
      final DataLoader<String, Object> semanticModelLoader =
          environment.getDataLoaderRegistry().getDataLoader(semanticModelTypeName);
      return membershipLoader
          .load(entityUrn)
          .thenCompose(
              semanticModelUrn -> {
                if (semanticModelUrn == null) {
                  return CompletableFuture.completedFuture(null);
                }
                return semanticModelLoader
                    .load(semanticModelUrn)
                    .thenApply(BulkEntitySemanticModelsResolver::unwrapSemanticModel);
              });
    };
  }

  @Nullable
  private static String extractUrn(final Object source) {
    if (source instanceof com.linkedin.datahub.graphql.generated.Entity) {
      return ((com.linkedin.datahub.graphql.generated.Entity) source).getUrn();
    }
    return null;
  }

  @Nullable
  private static SemanticModel unwrapSemanticModel(final Object result) {
    Object data = result;
    if (result instanceof graphql.execution.DataFetcherResult) {
      data = ((graphql.execution.DataFetcherResult<?>) result).getData();
    }
    if (data instanceof SemanticModel) {
      return (SemanticModel) data;
    }
    return null;
  }
}
