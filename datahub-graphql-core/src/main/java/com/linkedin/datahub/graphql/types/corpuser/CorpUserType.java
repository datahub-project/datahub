package com.linkedin.datahub.graphql.types.corpuser;

import com.linkedin.common.url.Url;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CorpUserUpdateInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.corpuser.mappers.CorpUserMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.Constants.*;


public class CorpUserType implements SearchableEntityType<CorpUser>, MutableType<CorpUserUpdateInput> {

    private final EntityClient _entityClient;

    public CorpUserType(final EntityClient entityClient) {
        _entityClient = entityClient;
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
    public List<DataFetcherResult<CorpUser>> batchLoad(final List<String> urns, final QueryContext context) {
        try {
            final List<Urn> corpUserUrns = urns
                    .stream()
                    .map(UrnUtils::getUrn)
                    .collect(Collectors.toList());

            final Map<Urn, EntityResponse> corpUserMap = _entityClient
                    .batchGetV2(CORP_USER_ENTITY_NAME, new HashSet<>(corpUserUrns), null,
                        context.getAuthentication());

            final List<EntityResponse> results = new ArrayList<>();
            for (Urn urn : corpUserUrns) {
                results.add(corpUserMap.getOrDefault(urn, null));
            }
            return results.stream()
                    .map(gmsCorpUser -> gmsCorpUser == null ? null
                        : DataFetcherResult.<CorpUser>newResult().data(CorpUserMapper.map(gmsCorpUser)).build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Datasets", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final SearchResult searchResult = _entityClient.search("corpuser", query, Collections.emptyMap(), start, count,
            context.getAuthentication());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final AutoCompleteResult result = _entityClient.autoComplete("corpuser", query, Collections.emptyMap(), limit, context.getAuthentication());
        return AutoCompleteResultsMapper.map(result);
    }

    private CorpuserUrn getCorpUserUrn(final String urnStr) {
        try {
            return CorpuserUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve user with urn %s, invalid urn", urnStr));
        }
    }

    public Class<CorpUserUpdateInput> inputClass() {
        return CorpUserUpdateInput.class;
    }

    @Override
    public CorpUser update(@Nonnull String urn, @Nonnull CorpUserUpdateInput input, @Nonnull QueryContext context) throws Exception {
        final CorpuserUrn actor = CorpuserUrn.createFromString(context.getAuthentication().getActor().toUrnStr());

        // Get existing editable info to merge with
        Optional<CorpUserEditableInfo> existingCorpUserEditableInfo =
            _entityClient.getVersionedAspect(urn, Constants.CORP_USER_EDITABLE_INFO_NAME, 0L, CorpUserEditableInfo.class,
                context.getAuthentication());

        // Create the MCP
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        proposal.setEntityUrn(Urn.createFromString(urn));
        proposal.setEntityType(Constants.CORP_USER_ENTITY_NAME);
        proposal.setAspectName(Constants.CORP_USER_EDITABLE_INFO_NAME);
        proposal.setAspect(GenericAspectUtils.serializeAspect(mapCorpUserEditableInfo(input, existingCorpUserEditableInfo)));
        proposal.setChangeType(ChangeType.UPSERT);
        _entityClient.ingestProposal(proposal, context.getAuthentication());

        return load(urn, context).getData();
    }

    private RecordTemplate mapCorpUserEditableInfo(CorpUserUpdateInput input, Optional<CorpUserEditableInfo> existing) {
        CorpUserEditableInfo result = existing.orElseGet(() -> new CorpUserEditableInfo());
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
        if (input.getPhone() != null) {
            result.setPhone(input.getPhone());
        }
        if (input.getSlack() != null) {
            result.setSlack(input.getSlack());
        }
        if (input.getEmail() != null) {
            result.setEmail(input.getEmail());
        }

        return result;
    }
}
