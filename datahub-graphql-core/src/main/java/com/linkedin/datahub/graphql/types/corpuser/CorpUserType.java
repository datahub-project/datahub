package com.linkedin.datahub.graphql.types.corpuser;

import static com.linkedin.datahub.graphql.Constants.DEFAULT_PERSONA_URNS;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CorpUserUpdateInput;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.corpuser.mappers.CorpUserMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CorpUserType
    implements SearchableEntityType<CorpUser, String>, MutableType<CorpUserUpdateInput, CorpUser> {

  private final EntityClient _entityClient;
  private final FeatureFlags _featureFlags;

  public CorpUserType(final EntityClient entityClient, final FeatureFlags featureFlags) {
    _entityClient = entityClient;
    _featureFlags = featureFlags;
  }

  @Override
  public Class<CorpUser> objectClass() {
    return CorpUser.class;
  }

  @Override
  public EntityType type() {
    return EntityType.CORP_USER;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public List<DataFetcherResult<CorpUser>> batchLoad(
      final List<String> urns, final QueryContext context) {
    try {
      final List<Urn> corpUserUrns =
          urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

      final Map<Urn, EntityResponse> corpUserMap =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              CORP_USER_ENTITY_NAME,
              new HashSet<>(corpUserUrns),
              null);

      final List<EntityResponse> results = new ArrayList<>(urns.size());
      for (Urn urn : corpUserUrns) {
        results.add(corpUserMap.getOrDefault(urn, null));
      }
      return results.stream()
          .map(
              gmsCorpUser ->
                  gmsCorpUser == null
                      ? null
                      : DataFetcherResult.<CorpUser>newResult()
                          .data(CorpUserMapper.map(context, gmsCorpUser, _featureFlags))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Datasets", e);
    }
  }

  @Override
  public SearchResults search(
      @Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      int count,
      @Nonnull final QueryContext context)
      throws Exception {
    final SearchResult searchResult =
        _entityClient.search(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            "corpuser",
            query,
            Collections.emptyMap(),
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
      @Nonnull final QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(
            context.getOperationContext(), "corpuser", query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }

  public Class<CorpUserUpdateInput> inputClass() {
    return CorpUserUpdateInput.class;
  }

  @Override
  public CorpUser update(
      @Nonnull String urn, @Nonnull CorpUserUpdateInput input, @Nonnull QueryContext context)
      throws Exception {
    if (isAuthorizedToUpdate(urn, input, context)) {
      // Get existing editable info to merge with
      Optional<CorpUserEditableInfo> existingCorpUserEditableInfo =
          _entityClient.getVersionedAspect(
              context.getOperationContext(),
              urn,
              CORP_USER_EDITABLE_INFO_NAME,
              0L,
              CorpUserEditableInfo.class);

      // Create the MCP
      final MetadataChangeProposal proposal =
          buildMetadataChangeProposalWithUrn(
              UrnUtils.getUrn(urn),
              CORP_USER_EDITABLE_INFO_NAME,
              mapCorpUserEditableInfo(input, existingCorpUserEditableInfo));
      _entityClient.ingestProposal(context.getOperationContext(), proposal, false);

      return load(urn, context).getData();
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private boolean isAuthorizedToUpdate(
      String urn, CorpUserUpdateInput input, QueryContext context) {
    // Decide whether the current principal should be allowed to update the Dataset.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = getAuthorizedPrivileges(input);

    // Either the updating actor is the user, or the actor has privileges to update the user
    // information.
    return context.getActorUrn().equals(urn)
        || AuthorizationUtils.isAuthorized(
            context,
            PoliciesConfig.CORP_GROUP_PRIVILEGES.getResourceType(),
            urn,
            orPrivilegeGroups);
  }

  private DisjunctivePrivilegeGroup getAuthorizedPrivileges(final CorpUserUpdateInput updateInput) {
    final ConjunctivePrivilegeGroup allPrivilegesGroup =
        new ConjunctivePrivilegeGroup(
            ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

    List<String> specificPrivileges = new ArrayList<>();
    if (updateInput.getSlack() != null
        || updateInput.getEmail() != null
        || updateInput.getPhone() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_CONTACT_INFO_PRIVILEGE.getType());
    } else if (updateInput.getAboutMe() != null
        || updateInput.getDisplayName() != null
        || updateInput.getPictureLink() != null
        || updateInput.getTeams() != null
        || updateInput.getTitle() != null) {
      specificPrivileges.add(PoliciesConfig.EDIT_USER_PROFILE_PRIVILEGE.getType());
    }

    final ConjunctivePrivilegeGroup specificPrivilegeGroup =
        new ConjunctivePrivilegeGroup(specificPrivileges);

    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    return new DisjunctivePrivilegeGroup(
        ImmutableList.of(allPrivilegesGroup, specificPrivilegeGroup));
  }

  private RecordTemplate mapCorpUserEditableInfo(
      CorpUserUpdateInput input, Optional<CorpUserEditableInfo> existing) {
    CorpUserEditableInfo result = existing.orElseGet(() -> new CorpUserEditableInfo());
    if (input.getDisplayName() != null) {
      result.setDisplayName(input.getDisplayName());
    }
    if (input.getAboutMe() != null) {
      result.setAboutMe(input.getAboutMe());
    }
    if (input.getPictureLink() != null) {
      result.setPictureLink(new Url(input.getPictureLink()));
    }
    if (input.getAboutMe() != null) {
      result.setAboutMe(input.getAboutMe());
    }
    if (input.getSkills() != null) {
      result.setSkills(new StringArray(input.getSkills()));
    }
    if (input.getTeams() != null) {
      result.setTeams(new StringArray(input.getTeams()));
    }
    if (input.getTitle() != null) {
      result.setTitle(input.getTitle());
    }
    if (input.getPhone() != null) {
      result.setPhone(input.getPhone());
    }
    if (input.getSlack() != null) {
      result.setSlack(input.getSlack());
    }
    if (input.getEmail() != null) {
      result.setEmail(input.getEmail());
    }
    if (input.getPlatformUrns() != null) {
      result.setPlatforms(
          new UrnArray(
              input.getPlatformUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList())));
    }
    if (input.getPersonaUrn() != null) {
      if (DEFAULT_PERSONA_URNS.contains(input.getPersonaUrn())) {
        result.setPersona(UrnUtils.getUrn(input.getPersonaUrn()));
      } else {
        throw new DataHubGraphQLException(
            String.format("Provided persona urn %s does not exist", input.getPersonaUrn()),
            DataHubGraphQLErrorCode.NOT_FOUND);
      }
    }
    return result;
  }
}
