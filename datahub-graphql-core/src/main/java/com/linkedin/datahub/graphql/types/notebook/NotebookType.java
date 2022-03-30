package com.linkedin.datahub.graphql.types.notebook;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.NotebookUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.Notebook;
import com.linkedin.datahub.graphql.generated.NotebookUpdateInput;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.types.notebook.mappers.NotebookMapper;
import com.linkedin.datahub.graphql.types.notebook.mappers.NotebookUpdateInputMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.Constants.*;
import static com.linkedin.metadata.Constants.*;

public class NotebookType implements SearchableEntityType<Notebook>, BrowsableEntityType<Notebook>, MutableType<NotebookUpdateInput, Notebook> {
  static final Set<String> ASPECTS_TO_RESOLVE = ImmutableSet.of(
      NOTEBOOK_KEY_ASPECT_NAME,
      NOTEBOOK_INFO_ASPECT_NAME,
      NOTEBOOK_CONTENT_ASPECT_NAME,
      EDITABLE_NOTEBOOK_PROPERTIES_ASPECT_NAME,
      OWNERSHIP_ASPECT_NAME,
      STATUS_ASPECT_NAME,
      GLOBAL_TAGS_ASPECT_NAME,
      GLOSSARY_TERMS_ASPECT_NAME,
      INSTITUTIONAL_MEMORY_ASPECT_NAME,
      DOMAINS_ASPECT_NAME,
      SUB_TYPES_ASPECT_NAME,
      DATA_PLATFORM_INSTANCE_ASPECT_NAME);

  private final EntityClient _entityClient;

  public NotebookType(EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public SearchResults search(@Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      int count,
      @Nonnull final QueryContext context) throws Exception {
    // Put empty map here according to
    // https://datahubspace.slack.com/archives/C029A3M079U/p1646288772126639
    final Map<String, String> facetFilters = Collections.emptyMap();
    final SearchResult searchResult = _entityClient.search(NOTEBOOK_ENTITY_NAME, query, facetFilters, start, count, context.getAuthentication());
    return UrnSearchResultsMapper.map(searchResult);
  }

  @Override
  public AutoCompleteResults autoComplete(@Nonnull String query,
      @Nullable String field,
      @Nullable List<FacetFilterInput> filters,
      int limit,
      @Nonnull final QueryContext context) throws Exception {
    // Put empty map here according to
    // https://datahubspace.slack.com/archives/C029A3M079U/p1646288772126639
    final Map<String, String> facetFilters = Collections.emptyMap();
    final AutoCompleteResult result = _entityClient.autoComplete(NOTEBOOK_ENTITY_NAME, query, facetFilters, limit, context.getAuthentication());
    return AutoCompleteResultsMapper.map(result);
  }

  @Override
  public BrowseResults browse(@Nonnull List<String> path, @Nullable List<FacetFilterInput> filters, int start,
      int count, @Nonnull QueryContext context) throws Exception {
    // Put empty map here according to
    // https://datahubspace.slack.com/archives/C029A3M079U/p1646288772126639
    final Map<String, String> facetFilters = Collections.emptyMap();

    final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
    final BrowseResult result = _entityClient.browse(NOTEBOOK_ENTITY_NAME, pathStr, facetFilters, start, count, context.getAuthentication());
    return BrowseResultMapper.map(result);
  }

  @Override
  public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull QueryContext context) throws Exception {
    final StringArray result = _entityClient.getBrowsePaths(NotebookUrn.createFromString(urn), context.getAuthentication());
    return BrowsePathsMapper.map(result);
  }

  @Override
  public EntityType type() {
    return EntityType.NOTEBOOK;
  }

  @Override
  public Class<Notebook> objectClass() {
    return Notebook.class;
  }

  @Override
  public List<DataFetcherResult<Notebook>> batchLoad(@Nonnull List<String> urnStrs, @Nonnull QueryContext context)
      throws Exception {
    final List<Urn> urns = urnStrs.stream()
        .map(UrnUtils::getUrn)
        .collect(Collectors.toList());
    try {
      final Map<Urn, EntityResponse> notebookMap = _entityClient.batchGetV2(NOTEBOOK_ENTITY_NAME, new HashSet<>(urns),
          ASPECTS_TO_RESOLVE, context.getAuthentication());

      return urns.stream()
          .map(urn -> notebookMap.getOrDefault(urn, null))
          .map(entityResponse -> entityResponse == null
              ? null
              : DataFetcherResult.<Notebook>newResult()
                  .data(NotebookMapper.map(entityResponse))
                  .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Notebook", e);
    }
  }

  @Override
  public Class<NotebookUpdateInput> inputClass() {
    return NotebookUpdateInput.class;
  }

  @Override
  public Notebook update(@Nonnull String urn, @Nonnull NotebookUpdateInput input, @Nonnull QueryContext context)
      throws Exception {
    if (!isAuthorized(urn, input, context)) {
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    CorpuserUrn actor = CorpuserUrn.createFromString(context.getAuthentication().getActor().toUrnStr());
    Collection<MetadataChangeProposal> proposals = NotebookUpdateInputMapper.map(input, actor);
    proposals.forEach(proposal -> proposal.setEntityUrn(UrnUtils.getUrn(urn)));

    try {
      _entityClient.batchIngestProposals(proposals, context.getAuthentication());
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(String.format("Failed to write entity with urn %s", urn), e);
    }

    return load(urn, context).getData();
  }

  private boolean isAuthorized(@Nonnull String urn, @Nonnull NotebookUpdateInput update, @Nonnull QueryContext context) {
    // Decide whether the current principal should be allowed to update the Dataset.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = getAuthorizedPrivileges(update);
    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getAuthentication().getActor().toUrnStr(),
        PoliciesConfig.NOTEBOOK_PRIVILEGES.getResourceType(),
        urn,
        orPrivilegeGroups);
  }

  private DisjunctivePrivilegeGroup getAuthorizedPrivileges(final NotebookUpdateInput updateInput) {

    final ConjunctivePrivilegeGroup allPrivilegesGroup = new ConjunctivePrivilegeGroup(ImmutableList.of(
      PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
    ));

    List<String> specificPrivileges = new ArrayList<>();
    if (updateInput.getOwnership() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE.getType());
    }
    if (updateInput.getEditableProperties() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_DOCS_PRIVILEGE.getType());
    }
    if (updateInput.getTags() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE.getType());
    }
    final ConjunctivePrivilegeGroup specificPrivilegeGroup = new ConjunctivePrivilegeGroup(specificPrivileges);

    // If you either have all entity privileges, or have the specific privileges required, you are authorized.
    return new DisjunctivePrivilegeGroup(ImmutableList.of(
        allPrivilegesGroup,
        specificPrivilegeGroup
    ));
  }
}
