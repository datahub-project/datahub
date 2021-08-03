package com.linkedin.datahub.graphql.types.dataset;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetSnapshotMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetUpdateInputMapper;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.dataset.client.Datasets;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;

import graphql.execution.DataFetcherResult;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;

public class DatasetType implements SearchableEntityType<Dataset>, BrowsableEntityType<Dataset>, MutableType<DatasetUpdateInput> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("origin", "platform");

    private final EntityClient _datasetsClient;

    public DatasetType(final EntityClient datasetsClient) {
        _datasetsClient = datasetsClient;
    }

    @Override
    public Class<Dataset> objectClass() {
        return Dataset.class;
    }

    @Override
    public Class<DatasetUpdateInput> inputClass() {
        return DatasetUpdateInput.class;
    }

    @Override
    public EntityType type() {
        return EntityType.DATASET;
    }

    @Override
    public List<DataFetcherResult<Dataset>> batchLoad(final List<String> urns, final QueryContext context) {

        final List<DatasetUrn> datasetUrns = urns.stream()
                .map(DatasetUtils::getDatasetUrn)
                .collect(Collectors.toList());

        try {
            final Map<Urn, Entity> datasetMap = _datasetsClient.batchGet(datasetUrns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));

            final List<Entity> gmsResults = new ArrayList<>();
            for (DatasetUrn urn : datasetUrns) {
                gmsResults.add(datasetMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                .map(gmsDataset ->
                    gmsDataset == null ? null : DataFetcherResult.<Dataset>newResult()
                        .data(DatasetSnapshotMapper.map(gmsDataset.getValue().getDatasetSnapshot()))
                        .localContext(AspectExtractor.extractAspects(gmsDataset.getValue().getDatasetSnapshot()))
                        .build()
                )
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
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final SearchResult searchResult = _datasetsClient.search("dataset", query, facetFilters, start, count);
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final AutoCompleteResult result = _datasetsClient.autoComplete("dataset", query, facetFilters, limit);
        return AutoCompleteResultsMapper.map(result);
    }

    @Override
    public BrowseResults browse(@Nonnull List<String> path,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
        final BrowseResult result = _datasetsClient.browse(
                "dataset",
                pathStr,
                facetFilters,
                start,
                count);
        return BrowseResultMapper.map(result);
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context) throws Exception {
        final StringArray result = _datasetsClient.getBrowsePaths(DatasetUtils.getDatasetUrn(urn));
        return BrowsePathsMapper.map(result);
    }

    @Override
    public Dataset update(@Nonnull DatasetUpdateInput input, @Nonnull QueryContext context) throws Exception {
        // TODO: Verify that updater is owner.
        final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActor());
        final com.linkedin.dataset.Dataset partialDataset = DatasetUpdateInputMapper.map(input, actor);
        partialDataset.setUrn(DatasetUrn.createFromString(input.getUrn()));


        // TODO: Migrate inner mappers to InputModelMappers & remove
        // Create Audit Stamp
        final AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(actor, SetMode.IGNORE_NULL);
        auditStamp.setTime(System.currentTimeMillis());

        if (partialDataset.hasDeprecation()) {
            partialDataset.getDeprecation().setActor(actor, SetMode.IGNORE_NULL);
        }

        if (partialDataset.hasEditableSchemaMetadata()) {
            partialDataset.getEditableSchemaMetadata().setLastModified(auditStamp);
            if (!partialDataset.getEditableSchemaMetadata().hasCreated()) {
                partialDataset.getEditableSchemaMetadata().setCreated(auditStamp);
            }
        }

        if (partialDataset.hasEditableProperties()) {
            partialDataset.getEditableProperties().setLastModified(auditStamp);
            if (!partialDataset.getEditableProperties().hasCreated()) {
                partialDataset.getEditableProperties().setCreated(auditStamp);
            }
        }

        partialDataset.setLastModified(auditStamp);

        try {
            Entity entity = new Entity();
            entity.setValue(Snapshot.create(Datasets.toSnapshot(partialDataset.getUrn(), partialDataset)));
            _datasetsClient.update(entity);
        } catch (RemoteInvocationException e) {
            throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
        }

        return load(input.getUrn(), context).getData();
    }
}
