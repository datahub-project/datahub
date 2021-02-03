package com.linkedin.datahub.graphql.types.dataset;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMetadataMapper;
import com.linkedin.datahub.graphql.types.mappers.DatasetMapper;
import com.linkedin.datahub.graphql.types.mappers.SearchResultsMapper;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.dataset.client.Datasets;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.restli.common.CollectionResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;

public class DatasetType implements SearchableEntityType<Dataset>, BrowsableEntityType<Dataset> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("origin", "platform");
    private static final String DEFAULT_AUTO_COMPLETE_FIELD = "name";

    private final Datasets _datasetsClient;

    public DatasetType(final Datasets datasetsClient) {
        _datasetsClient = datasetsClient;
    }

    @Override
    public Class<Dataset> objectClass() {
        return Dataset.class;
    }

    @Override
    public EntityType type() {
        return EntityType.DATASET;
    }

    @Override
    public List<Dataset> batchLoad(final List<String> urns) {

        final List<DatasetUrn> datasetUrns = urns.stream()
                .map(DatasetUtils::getDatasetUrn)
                .collect(Collectors.toList());

        try {
            final Map<DatasetUrn, com.linkedin.dataset.Dataset> datasetMap = _datasetsClient.batchGet(datasetUrns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));

            final List<com.linkedin.dataset.Dataset> gmsResults = new ArrayList<>();
            for (DatasetUrn urn : datasetUrns) {
                gmsResults.add(datasetMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsDataset -> gmsDataset == null ? null : DatasetMapper.map(gmsDataset))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Datasets", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final CollectionResponse<com.linkedin.dataset.Dataset> searchResult = _datasetsClient.search(query, facetFilters, start, count);
        return SearchResultsMapper.map(searchResult, DatasetMapper::map);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        field = field != null ? field : DEFAULT_AUTO_COMPLETE_FIELD;
        final AutoCompleteResult result = _datasetsClient.autoComplete(query, field, facetFilters, limit);
        return AutoCompleteResultsMapper.map(result);
    }

    @Override
    public BrowseResults browse(@Nonnull List<String> path,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final BrowseResult result = _datasetsClient.browse(
                BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path),
                facetFilters,
                start,
                count);
        final List<String> urns = result.getEntities().stream().map(entity -> entity.getUrn().toString()).collect(Collectors.toList());
        final List<Dataset> datasets = batchLoad(urns);
        final BrowseResults browseResults = new BrowseResults();
        browseResults.setStart(result.getFrom());
        browseResults.setCount(result.getPageSize());
        browseResults.setTotal(result.getNumEntities());
        browseResults.setMetadata(BrowseResultMetadataMapper.map(result.getMetadata()));
        browseResults.setEntities(datasets.stream()
                .map(dataset -> (com.linkedin.datahub.graphql.generated.Entity) dataset)
                .collect(Collectors.toList()));
        return browseResults;
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn) throws Exception {
        final StringArray result = _datasetsClient.getBrowsePaths(DatasetUtils.getDatasetUrn(urn));
        return BrowsePathsMapper.map(result);
    }
}
