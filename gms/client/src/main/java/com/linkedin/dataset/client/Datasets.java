package com.linkedin.dataset.client;

import com.linkedin.common.Status;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.DatasetsDoAutocompleteRequestBuilder;
import com.linkedin.dataset.DatasetsDoBrowseRequestBuilder;
import com.linkedin.dataset.DatasetsDoGetBrowsePathsRequestBuilder;
import com.linkedin.dataset.DatasetsDoGetSnapshotRequestBuilder;
import com.linkedin.dataset.DatasetsFindByFilterRequestBuilder;
import com.linkedin.dataset.DatasetsFindBySearchRequestBuilder;
import com.linkedin.dataset.DatasetsRequestBuilders;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.DatasetActionRequestBuilder;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseBrowsableClient;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.BatchGetEntityRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.QueryUtils.*;

public class Datasets extends BaseBrowsableClient<Dataset, DatasetUrn> {
    private static final DatasetsRequestBuilders DATASETS_REQUEST_BUILDERS = new DatasetsRequestBuilders();
    private static final DatasetActionRequestBuilder DATASET_ACTION_REQUEST_BUILDERS = new DatasetActionRequestBuilder();

    public Datasets(@Nonnull Client restliClient) {
        super(restliClient);
    }

    /**
     * Gets {@link Dataset} model for the given urn
     *
     * @param urn dataset urn
     * @return {@link Dataset} dataset model
     * @throws RemoteInvocationException
     */
    @Nonnull
    public Dataset get(@Nonnull DatasetUrn urn)
            throws RemoteInvocationException {
        GetRequest<Dataset> getRequest = DATASETS_REQUEST_BUILDERS.get()
                .id(new ComplexResourceKey<>(toDatasetKey(urn), new EmptyRecord()))
                .build();

        return _client.sendRequest(getRequest).getResponse().getEntity();
    }

    /**
     * Searches for datasets matching to a given query and filters
     *
     * @param input search query
     * @param requestFilters search filters
     * @param start start offset for search results
     * @param count max number of search results requested
     * @return Snapshot key
     * @throws RemoteInvocationException
     */
    @Nonnull
    public CollectionResponse<Dataset> search(@Nonnull String input, @Nonnull Map<String, String> requestFilters,
                                               int start, int count) throws RemoteInvocationException {

        return search(input, null, requestFilters, null, start, count);
    }

    @Override
    @Nonnull
    public CollectionResponse<Dataset> search(@Nonnull String input, @Nullable StringArray aspectNames,
        @Nullable Map<String, String> requestFilters, @Nullable SortCriterion sortCriterion, int start, int count)
        throws RemoteInvocationException {

        final DatasetsFindBySearchRequestBuilder requestBuilder = DATASETS_REQUEST_BUILDERS.findBySearch()
            .inputParam(input)
            .aspectsParam(aspectNames)
            .filterParam(newFilter(requestFilters))
            .sortParam(sortCriterion)
            .paginate(start, count);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Gets browse snapshot of a given path
     *
     * @param query search query
     * @param field field of the dataset
     * @param requestFilters autocomplete filters
     * @param limit max number of autocomplete results
     * @throws RemoteInvocationException
     */
    @Nonnull
    public AutoCompleteResult autoComplete(@Nonnull String query, @Nonnull String field,
                                           @Nonnull Map<String, String> requestFilters,
                                           @Nonnull int limit) throws RemoteInvocationException {
        DatasetsDoAutocompleteRequestBuilder requestBuilder = DATASETS_REQUEST_BUILDERS
                .actionAutocomplete()
                .queryParam(query)
                .fieldParam(field)
                .filterParam(newFilter(requestFilters))
                .limitParam(limit);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Gets browse snapshot of a given path
     *
     * @param path path being browsed
     * @param requestFilters browse filters
     * @param start start offset of first dataset
     * @param limit max number of datasets
     * @throws RemoteInvocationException
     */
    @Nonnull
    @Override
    public BrowseResult browse(@Nonnull String path, @Nullable Map<String, String> requestFilters,
                               int start, int limit) throws RemoteInvocationException {
        DatasetsDoBrowseRequestBuilder requestBuilder = DATASETS_REQUEST_BUILDERS
                .actionBrowse()
                .pathParam(path)
                .startParam(start)
                .limitParam(limit);
        if (requestFilters != null) {
            requestBuilder.filterParam(newFilter(requestFilters));
        }
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Gets list of dataset urns from strongly consistent local secondary index
     *
     * @param lastUrn last dataset urn of the previous fetched page. For the first page, this will be NULL
     * @param size size of the page that needs to be fetched
     * @return list of dataset urns represented as {@link StringArray}
     * @throws RemoteInvocationException
     */
    @Nonnull
    public List<String> listUrnsFromIndex(@Nullable DatasetUrn lastUrn, int size) throws RemoteInvocationException {
        return listUrnsFromIndex(null, lastUrn, size);
    }

    /**
     * Gets list of dataset urns from strongly consistent local secondary index given an {@link IndexFilter} specifying filter conditions
     *
     * @param indexFilter index filter that defines the filter conditions
     * @param lastUrn last dataset urn of the previous fetched page. For the first page, this will be NULL
     * @param size size of the page that needs to be fetched
     * @return list of dataset urns represented as {@link StringArray}
     * @throws RemoteInvocationException
     */
    @Nonnull
    public List<String> listUrnsFromIndex(@Nullable IndexFilter indexFilter, @Nullable DatasetUrn lastUrn, int size)
        throws RemoteInvocationException {
        final List<Dataset> response = filter(indexFilter, Collections.emptyList(), lastUrn, size);
        return response.stream()
            .map(dataset -> new DatasetUrn(dataset.getPlatform(), dataset.getName(), dataset.getOrigin()))
            .map(Urn::toString)
            .collect(Collectors.toList());
    }

    /**
     * Gets a list of {@link Dataset} whose raw metadata contains the list of dataset urns from strongly consistent
     * local secondary index that satisfy the filter conditions provided in {@link IndexFilter}
     *
     * @param indexFilter {@link IndexFilter} that specifies the filter conditions for urns to be fetched from secondary index
     * @param aspectNames list of aspects whose value should be retrieved
     * @param lastUrn last dataset urn of the previous fetched page. For the first page, this will be NULL
     * @param size size of the page that needs to be fetched
     * @return collection of {@link Dataset} whose raw metadata contains the list of filtered dataset urns
     * @throws RemoteInvocationException
     */
    @Nonnull
    public List<Dataset> filter(@Nullable IndexFilter indexFilter, @Nullable List<String> aspectNames,
        @Nullable DatasetUrn lastUrn, int size) throws RemoteInvocationException {
        final DatasetsFindByFilterRequestBuilder requestBuilder =
            DATASETS_REQUEST_BUILDERS.findByFilter().filterParam(indexFilter).aspectsParam(aspectNames).paginate(0, size);
        if (lastUrn != null) {
            requestBuilder.urnParam(lastUrn.toString());
        }
        return _client.sendRequest(requestBuilder.build()).getResponseEntity().getElements();
    }

    /**
     * Gets browse path(s) given dataset urn
     *
     * @param urn urn for the entity
     * @return list of paths given urn
     * @throws RemoteInvocationException
     */
    @Nonnull
    public StringArray getBrowsePaths(@Nonnull DatasetUrn urn) throws RemoteInvocationException {
        DatasetsDoGetBrowsePathsRequestBuilder requestBuilder = DATASETS_REQUEST_BUILDERS
                .actionGetBrowsePaths()
                .urnParam(urn);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Batch gets list of {@link Dataset} models
     *
     * @param urns list of dataset urn
     * @return map of {@link Dataset} models
     * @throws RemoteInvocationException
     */
    @Nonnull
    public Map<DatasetUrn, Dataset> batchGet(@Nonnull Set<DatasetUrn> urns)
        throws RemoteInvocationException {
        BatchGetEntityRequest<ComplexResourceKey<DatasetKey, EmptyRecord>, Dataset> batchGetRequest
            = DATASETS_REQUEST_BUILDERS.batchGet()
            .ids(urns.stream().map(this::getKeyFromUrn).collect(Collectors.toSet()))
            .build();

        return _client.sendRequest(batchGetRequest).getResponseEntity().getResults()
            .entrySet().stream().collect(Collectors.toMap(
                entry -> getUrnFromKey(entry.getKey()),
                entry -> entry.getValue().getEntity())
            );
    }

    /**
     * Gets latest full dataset snapshot given dataset urn
     *
     * @param datasetUrn dataset urn
     * @return latest full dataset snapshot
     * @throws RemoteInvocationException
     */
    public DatasetSnapshot getLatestFullSnapshot(@Nonnull DatasetUrn datasetUrn) throws RemoteInvocationException {
        DatasetsDoGetSnapshotRequestBuilder requestBuilder = DATASETS_REQUEST_BUILDERS
                .actionGetSnapshot()
                .urnParam(datasetUrn.toString());
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    @Nonnull
    private ComplexResourceKey<DatasetKey, EmptyRecord> getKeyFromUrn(@Nonnull DatasetUrn urn) {
        return new ComplexResourceKey<>(toDatasetKey(urn), new EmptyRecord());
    }

    @Nonnull
    private DatasetUrn getUrnFromKey(@Nonnull ComplexResourceKey<DatasetKey, EmptyRecord> key) {
        return toDatasetUrn(key.getKey());
    }

    @Nonnull
    private DatasetKey toDatasetKey(@Nonnull DatasetUrn urn) {
        return new DatasetKey()
            .setName(urn.getDatasetNameEntity())
            .setOrigin(urn.getOriginEntity())
            .setPlatform(urn.getPlatformEntity());
    }

    @Nonnull
    private DatasetUrn toDatasetUrn(@Nonnull DatasetKey key) {
        return new DatasetUrn(key.getPlatform(), key.getName(), key.getOrigin());
    }

    /**
     * Update an existing Dataset
     */
    public void update(@Nonnull final DatasetUrn urn, @Nonnull final Dataset dataset) throws RemoteInvocationException {
        Request request = DATASET_ACTION_REQUEST_BUILDERS.createRequest(urn, toSnapshot(urn, dataset));
        _client.sendRequest(request).getResponse();
    }

    // Copied from an unused method in Datasets resource.
    static DatasetSnapshot toSnapshot(@Nonnull final DatasetUrn datasetUrn, @Nonnull final Dataset dataset) {
        final List<DatasetAspect> aspects = new ArrayList<>();
        if (dataset.getProperties() != null) {
            aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, getDatasetPropertiesAspect(dataset)));
        }
        if (dataset.getDeprecation() != null) {
            aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getDeprecation()));
        }
        if (dataset.getInstitutionalMemory() != null) {
            aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getInstitutionalMemory()));
        }
        if (dataset.getOwnership() != null) {
            aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getOwnership()));
        }
        if (dataset.getSchemaMetadata() != null) {
            aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getSchemaMetadata()));
        }
        if (dataset.getStatus() != null) {
            aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getStatus()));
        }
        if (dataset.getUpstreamLineage() != null) {
            aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getUpstreamLineage()));
        }
        if (dataset.hasRemoved()) {
            aspects.add(DatasetAspect.create(new Status().setRemoved(dataset.isRemoved())));
        }
        if (dataset.getGlobalTags() != null) {
            aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getGlobalTags()));
        }
        if (dataset.getEditableSchemaMetadata() != null) {
            aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getEditableSchemaMetadata()));
        }
        return ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn, aspects);
    }

    private static DatasetProperties getDatasetPropertiesAspect(@Nonnull Dataset dataset) {
        final DatasetProperties datasetProperties = new DatasetProperties();
        datasetProperties.setDescription(dataset.getDescription());
        datasetProperties.setTags(dataset.getTags());
        if (dataset.getUri() != null)  {
            datasetProperties.setUri(dataset.getUri());
        }
        if (dataset.getProperties() != null) {
            datasetProperties.setCustomProperties(dataset.getProperties());
        }
        if (dataset.getExternalUrl() != null) {
            datasetProperties.setExternalUrl(dataset.getExternalUrl());
        }
        return datasetProperties;
    }
}
