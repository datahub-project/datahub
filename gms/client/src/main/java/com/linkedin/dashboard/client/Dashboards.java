package com.linkedin.dashboard.client;

import com.linkedin.BatchGetUtils;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.dashboard.Dashboard;
import com.linkedin.dashboard.DashboardKey;
import com.linkedin.dashboard.DashboardsDoAutocompleteRequestBuilder;
import com.linkedin.dashboard.DashboardsDoBrowseRequestBuilder;
import com.linkedin.dashboard.DashboardsDoGetBrowsePathsRequestBuilder;
import com.linkedin.dashboard.DashboardsFindBySearchRequestBuilder;
import com.linkedin.dashboard.DashboardsRequestBuilders;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.DashboardAspect;
import com.linkedin.metadata.configs.DashboardSearchConfig;
import com.linkedin.metadata.dao.DashboardActionRequestBuilder;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseBrowsableClient;
import com.linkedin.metadata.snapshot.DashboardSnapshot;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;


public class Dashboards extends BaseBrowsableClient<Dashboard, DashboardUrn> {

    private static final DashboardsRequestBuilders DASHBOARDS_REQUEST_BUILDERS = new DashboardsRequestBuilders();
    private static final DashboardActionRequestBuilder DASHBOARDS_ACTION_REQUEST_BUILDERS = new DashboardActionRequestBuilder();
    private static final DashboardSearchConfig DASHBOARDS_SEARCH_CONFIG = new DashboardSearchConfig();

    public Dashboards(@Nonnull Client restliClient) {
        super(restliClient);
    }

    @Nonnull
    public Dashboard get(@Nonnull DashboardUrn urn)
            throws RemoteInvocationException {
        GetRequest<Dashboard> getRequest = DASHBOARDS_REQUEST_BUILDERS.get()
                .id(new ComplexResourceKey<>(toDashboardsKey(urn), new EmptyRecord()))
                .build();

        return _client.sendRequest(getRequest).getResponse().getEntity();
    }

    /**
     * Gets browse path(s) given dataset urn
     *
     * @param urn urn for the entity
     * @return list of paths given urn
     * @throws RemoteInvocationException
     */
    @Nonnull
    public StringArray getBrowsePaths(@Nonnull DashboardUrn urn) throws RemoteInvocationException {
        DashboardsDoGetBrowsePathsRequestBuilder requestBuilder = DASHBOARDS_REQUEST_BUILDERS
            .actionGetBrowsePaths()
            .urnParam(urn);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    @Nonnull
    public Map<DashboardUrn, Dashboard> batchGet(@Nonnull Set<DashboardUrn> urns)
            throws RemoteInvocationException {
        return BatchGetUtils.batchGet(
                urns,
                DASHBOARDS_REQUEST_BUILDERS.batchGet(),
                this::getKeyFromUrn,
                this::getUrnFromKey,
                _client
        );
    }

    @Nonnull
    @Override
    public CollectionResponse<Dashboard> search(@Nonnull String input,
                                                @Nullable StringArray aspectNames,
                                                @Nullable Map<String, String> requestFilters,
                                                @Nullable SortCriterion sortCriterion,
                                                int start,
                                                int count)
            throws RemoteInvocationException {
        final DashboardsFindBySearchRequestBuilder requestBuilder = DASHBOARDS_REQUEST_BUILDERS.findBySearch()
                .aspectsParam(aspectNames)
                .inputParam(input)
                .sortParam(sortCriterion)
                .paginate(start, count);
        if (requestFilters != null) {
            requestBuilder.filterParam(newFilter(requestFilters));
        }
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    @Nonnull
    public CollectionResponse<Dashboard> search(@Nonnull String input, int start, int count)
            throws RemoteInvocationException {
        return search(input, null, null, start, count);
    }

    @Nonnull
    @Override
    public AutoCompleteResult autocomplete(@Nonnull String query, @Nullable String field, @Nonnull Map<String, String> requestFilters, int limit)
            throws RemoteInvocationException {
        final String autocompleteField = (field != null) ? field : DASHBOARDS_SEARCH_CONFIG.getDefaultAutocompleteField();
        DashboardsDoAutocompleteRequestBuilder requestBuilder = DASHBOARDS_REQUEST_BUILDERS
                .actionAutocomplete()
                .queryParam(query)
                .fieldParam(autocompleteField)
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
        DashboardsDoBrowseRequestBuilder requestBuilder = DASHBOARDS_REQUEST_BUILDERS
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
     * Update an existing Dashboard
     */
    public void update(@Nonnull final DashboardUrn urn, @Nonnull final Dashboard dashboard) throws RemoteInvocationException {
        Request request = DASHBOARDS_ACTION_REQUEST_BUILDERS.createRequest(urn, toSnapshot(dashboard, urn));
        _client.sendRequest(request).getResponse();
    }

    static DashboardSnapshot toSnapshot(@Nonnull Dashboard dashboard, @Nonnull DashboardUrn urn) {
        final List<DashboardAspect> aspects = new ArrayList<>();
        if (dashboard.hasInfo()) {
            aspects.add(ModelUtils.newAspectUnion(DashboardAspect.class, dashboard.getInfo()));
        }
        if (dashboard.hasOwnership()) {
            aspects.add(ModelUtils.newAspectUnion(DashboardAspect.class, dashboard.getOwnership()));
        }
        if (dashboard.hasStatus()) {
            aspects.add(ModelUtils.newAspectUnion(DashboardAspect.class, dashboard.getStatus()));
        }
        if (dashboard.hasGlobalTags()) {
            aspects.add(ModelUtils.newAspectUnion(DashboardAspect.class, dashboard.getGlobalTags()));
        }
        return ModelUtils.newSnapshot(DashboardSnapshot.class, urn, aspects);
    }

    @Nonnull
    private ComplexResourceKey<DashboardKey, EmptyRecord> getKeyFromUrn(@Nonnull DashboardUrn urn) {
        return new ComplexResourceKey<>(toDashboardsKey(urn), new EmptyRecord());
    }

    @Nonnull
    private DashboardUrn getUrnFromKey(@Nonnull ComplexResourceKey<DashboardKey, EmptyRecord> key) {
        return toDashboardsUrn(key.getKey());
    }

    @Nonnull
    private DashboardKey toDashboardsKey(@Nonnull DashboardUrn urn) {
        return new DashboardKey().setTool(urn.getDashboardToolEntity()).setDashboardId(urn.getDashboardIdEntity());
    }

    @Nonnull
    private DashboardUrn toDashboardsUrn(@Nonnull DashboardKey key) {
        return new DashboardUrn(key.getTool(), key.getDashboardId());
    }
}
