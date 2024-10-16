package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;
import static org.apache.commons.lang3.StringUtils.isBlank;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteMultipleInput;
import com.linkedin.datahub.graphql.generated.AutoCompleteMultipleResults;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolver responsible for resolving the 'autocomplete' field of the Query type */
public class AutoCompleteForMultipleResolver
    implements DataFetcher<CompletableFuture<AutoCompleteMultipleResults>> {

  private static final Logger _logger =
      LoggerFactory.getLogger(AutoCompleteForMultipleResolver.class.getName());

  private final Map<EntityType, SearchableEntityType<?, ?>> _typeToEntity;
  private final ViewService _viewService;

  public AutoCompleteForMultipleResolver(
      @Nonnull final List<SearchableEntityType<?, ?>> searchableEntities,
      @Nonnull final ViewService viewService) {
    _typeToEntity =
        searchableEntities.stream()
            .collect(Collectors.toMap(SearchableEntityType::type, entity -> entity));
    _viewService = viewService;
  }

  @Override
  public CompletableFuture<AutoCompleteMultipleResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final AutoCompleteMultipleInput input =
        bindArgument(environment.getArgument("input"), AutoCompleteMultipleInput.class);

    if (isBlank(input.getQuery())) {
      _logger.error("'query' parameter was null or empty");
      throw new ValidationException("'query' parameter can not be null or empty");
    }
    // escape forward slash since it is a reserved character in Elasticsearch
    final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());
    final DataHubViewInfo maybeResolvedView =
        (input.getViewUrn() != null)
            ? resolveView(
                context.getOperationContext(), _viewService, UrnUtils.getUrn(input.getViewUrn()))
            : null;

    List<EntityType> types = getEntityTypes(input.getTypes(), maybeResolvedView);
    types =
        types != null
            ? types.stream()
                .filter(AUTO_COMPLETE_ENTITY_TYPES::contains)
                .collect(Collectors.toList())
            : null;
    if (types != null && types.size() > 0) {
      return AutocompleteUtils.batchGetAutocompleteResults(
          types.stream()
              .map(_typeToEntity::get)
              .filter(Objects::nonNull)
              .collect(Collectors.toList()),
          sanitizedQuery,
          input,
          environment,
          maybeResolvedView);
    }

    // By default, autocomplete only against the Default Set of Autocomplete entities
    return AutocompleteUtils.batchGetAutocompleteResults(
        AUTO_COMPLETE_ENTITY_TYPES.stream()
            .map(_typeToEntity::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toList()),
        sanitizedQuery,
        input,
        environment,
        maybeResolvedView);
  }

  /** Gets the intersection of provided input types and types on the view applied (if any) */
  @Nullable
  List<EntityType> getEntityTypes(
      final @Nullable List<EntityType> inputTypes,
      final @Nullable DataHubViewInfo maybeResolvedView) {
    List<EntityType> types = inputTypes;
    if (maybeResolvedView != null) {
      List<EntityType> inputEntityTypes = types != null ? types : new ArrayList<>();
      final List<String> inputEntityNames =
          inputEntityTypes.stream().map(EntityTypeMapper::getName).collect(Collectors.toList());
      List<String> stringEntityTypes =
          SearchUtils.intersectEntityTypes(
              inputEntityNames, maybeResolvedView.getDefinition().getEntityTypes());

      types =
          stringEntityTypes.stream().map(EntityTypeMapper::getType).collect(Collectors.toList());
    }

    return types;
  }
}
