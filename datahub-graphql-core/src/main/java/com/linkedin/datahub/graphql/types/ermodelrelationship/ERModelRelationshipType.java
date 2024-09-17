package com.linkedin.datahub.graphql.types.ermodelrelationship;

import static com.linkedin.datahub.graphql.Constants.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.ERModelRelationshipUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.ERModelRelationship;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipUpdateInput;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.ermodelrelationship.mappers.ERModelRelationMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ERModelRelationshipType
    implements com.linkedin.datahub.graphql.types.EntityType<ERModelRelationship, String>,
        BrowsableEntityType<ERModelRelationship, String>,
        SearchableEntityType<ERModelRelationship, String> {

  static final Set<String> ASPECTS_TO_RESOLVE =
      ImmutableSet.of(
          ER_MODEL_RELATIONSHIP_KEY_ASPECT_NAME,
          ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME,
          EDITABLE_ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME,
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          GLOBAL_TAGS_ASPECT_NAME,
          GLOSSARY_TERMS_ASPECT_NAME);

  private static final Set<String> FACET_FIELDS = ImmutableSet.of("name");
  private static final String ENTITY_NAME = "erModelRelationship";

  private final EntityClient _entityClient;
  private final FeatureFlags _featureFlags;

  public ERModelRelationshipType(final EntityClient entityClient, final FeatureFlags featureFlags) {
    _entityClient = entityClient;
    _featureFlags =
        featureFlags; // TODO: check if ERModelRelation Feture is Enabled and throw error when
    // called
  }

  @Override
  public Class<ERModelRelationship> objectClass() {
    return ERModelRelationship.class;
  }

  @Override
  public EntityType type() {
    return EntityType.ER_MODEL_RELATIONSHIP;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public List<DataFetcherResult<ERModelRelationship>> batchLoad(
      @Nonnull final List<String> urns, @Nonnull final QueryContext context) throws Exception {
    final List<Urn> ermodelrelationUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              ER_MODEL_RELATIONSHIP_ENTITY_NAME,
              new HashSet<>(ermodelrelationUrns),
              ASPECTS_TO_RESOLVE);

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : ermodelrelationUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<ERModelRelationship>newResult()
                          .data(ERModelRelationMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to load erModelRelationship entity", e);
    }
  }

  @Nonnull
  @Override
  public BrowseResults browse(
      @Nonnull List<String> path,
      @Nullable List<FacetFilterInput> filters,
      int start,
      int count,
      @Nonnull QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final String pathStr =
        path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
    final BrowseResult result =
        _entityClient.browse(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(false)),
            "erModelRelationship",
            pathStr,
            facetFilters,
            start,
            count);
    return BrowseResultMapper.map(context, result);
  }

  @Nonnull
  @Override
  public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull QueryContext context)
      throws Exception {
    final StringArray result =
        _entityClient.getBrowsePaths(context.getOperationContext(), UrnUtils.getUrn(urn));
    return BrowsePathsMapper.map(context, result);
  }

  @Override
  public SearchResults search(
      @Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      int count,
      @Nonnull QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final SearchResult searchResult =
        _entityClient.search(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            ENTITY_NAME,
            query,
            facetFilters,
            start,
            count);
    return UrnSearchResultsMapper.map(context, searchResult);
  }

  @Override
  public AutoCompleteResults autoComplete(
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter filters,
      int limit,
      @Nonnull QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(
            context.getOperationContext(), ENTITY_NAME, query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }

  public static boolean canUpdateERModelRelation(
      @Nonnull QueryContext context,
      ERModelRelationshipUrn resourceUrn,
      ERModelRelationshipUpdateInput updateInput) {
    final ConjunctivePrivilegeGroup editPrivilegesGroup =
        new ConjunctivePrivilegeGroup(
            ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));
    List<String> specificPrivileges = new ArrayList<>();
    if (updateInput.getEditableProperties() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_DOCS_PRIVILEGE.getType());
    }
    final ConjunctivePrivilegeGroup specificPrivilegeGroup =
        new ConjunctivePrivilegeGroup(specificPrivileges);

    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(editPrivilegesGroup, specificPrivilegeGroup));
    return AuthorizationUtils.isAuthorized(
        context, resourceUrn.getEntityType(), resourceUrn.toString(), orPrivilegeGroups);
  }

  public static boolean canCreateERModelRelation(
      @Nonnull QueryContext context, Urn sourceUrn, Urn destinationUrn) {
    final ConjunctivePrivilegeGroup editPrivilegesGroup =
        new ConjunctivePrivilegeGroup(
            ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));
    final ConjunctivePrivilegeGroup createPrivilegesGroup =
        new ConjunctivePrivilegeGroup(
            ImmutableList.of(PoliciesConfig.CREATE_ER_MODEL_RELATIONSHIP_PRIVILEGE.getType()));
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(ImmutableList.of(editPrivilegesGroup, createPrivilegesGroup));
    boolean sourcePrivilege =
        AuthorizationUtils.isAuthorized(
            context, sourceUrn.getEntityType(), sourceUrn.toString(), orPrivilegeGroups);
    boolean destinationPrivilege =
        AuthorizationUtils.isAuthorized(
            context, destinationUrn.getEntityType(), destinationUrn.toString(), orPrivilegeGroups);
    return sourcePrivilege && destinationPrivilege;
  }
}
