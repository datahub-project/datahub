package com.linkedin.datahub.graphql.types.datajob;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.datajob.mappers.DataJobMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.generated.DataJobUpdateInput;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.datajob.mappers.DataJobUpdateInputMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMetadataMapper;
import com.linkedin.datahub.graphql.types.mappers.SearchResultsMapper;
import com.linkedin.datajob.client.DataJobs;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.restli.common.CollectionResponse;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.linkedin.r2.RemoteInvocationException;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;


public class DataJobType implements SearchableEntityType<DataJob>, BrowsableEntityType<DataJob>, MutableType<DataJobUpdateInput> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("flow");
    private static final String DEFAULT_AUTO_COMPLETE_FIELD = "jobId";
    private final DataJobs _dataJobsClient;

    public DataJobType(final DataJobs dataJobsClient) {
        _dataJobsClient = dataJobsClient;
    }

    @Override
    public EntityType type() {
        return EntityType.DATA_JOB;
    }

    @Override
    public Class<DataJob> objectClass() {
        return DataJob.class;
    }

    @Override
    public Class<DataJobUpdateInput> inputClass() {
        return DataJobUpdateInput.class;
    }

    @Override
    public List<DataJob> batchLoad(final List<String> urns, final QueryContext context) throws Exception {
        final List<DataJobUrn> dataJobUrns = urns.stream()
            .map(this::getDataJobUrn)
            .collect(Collectors.toList());

        try {
            final Map<DataJobUrn, com.linkedin.datajob.DataJob> dataJobMap = _dataJobsClient.batchGet(dataJobUrns
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

            final List<com.linkedin.datajob.DataJob> gmsResults = dataJobUrns.stream()
                .map(jobUrn -> dataJobMap.getOrDefault(jobUrn, null)).collect(Collectors.toList());

            return gmsResults.stream()
                .map(gmsDataJob -> gmsDataJob == null ? null : DataJobMapper.map(gmsDataJob))
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load DataJobs", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final CollectionResponse<com.linkedin.datajob.DataJob> searchResult = _dataJobsClient.search(query, facetFilters, start, count);
        return SearchResultsMapper.map(searchResult, DataJobMapper::map);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        field = field != null ? field : DEFAULT_AUTO_COMPLETE_FIELD;
        final AutoCompleteResult result = _dataJobsClient.autoComplete(query, field, facetFilters, limit);
        return AutoCompleteResultsMapper.map(result);
    }

    private DataJobUrn getDataJobUrn(String urnStr) {
        try {
            return DataJobUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve datajob with urn %s, invalid urn", urnStr));
        }
    }

    @Override
    public BrowseResults browse(@Nonnull List<String> path, @Nullable List<FacetFilterInput> filters, int start,
        int count, @Nonnull QueryContext context) throws Exception {
                final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
        final BrowseResult result = _dataJobsClient.browse(
                pathStr,
                facetFilters,
                start,
                count);
        final List<String> urns = result.getEntities().stream().map(entity -> entity.getUrn().toString()).collect(Collectors.toList());
        final List<DataJob> dataJobs = batchLoad(urns, context);
        final BrowseResults browseResults = new BrowseResults();
        browseResults.setStart(result.getFrom());
        browseResults.setCount(result.getPageSize());
        browseResults.setTotal(result.getNumEntities());
        browseResults.setMetadata(BrowseResultMetadataMapper.map(result.getMetadata()));
        browseResults.setEntities(dataJobs.stream()
                .map(dataset -> (com.linkedin.datahub.graphql.generated.Entity) dataset)
                .collect(Collectors.toList()));
        return browseResults;
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull QueryContext context) throws Exception {
        final StringArray result = _dataJobsClient.getBrowsePaths(DataJobUrn.createFromString(urn));
        return BrowsePathsMapper.map(result);
    }

    @Override
    public DataJob update(@Nonnull DataJobUpdateInput input, @Nonnull QueryContext context) throws Exception {

        final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActor());
        final com.linkedin.datajob.DataJob partialDataJob = DataJobUpdateInputMapper.map(input, actor);

        try {
            _dataJobsClient.update(DataJobUrn.createFromString(input.getUrn()), partialDataJob);
        } catch (RemoteInvocationException e) {
            throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
        }

        return load(input.getUrn(), context);
    }
}
