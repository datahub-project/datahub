package com.linkedin.dashboard.client;

import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.dashboard.Dashboard;
import com.linkedin.dashboard.DashboardKey;
import com.linkedin.dashboard.DashboardsDoAutocompleteRequestBuilder;
import com.linkedin.dashboard.DashboardsFindBySearchRequestBuilder;
import com.linkedin.dashboard.DashboardsRequestBuilders;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.configs.DashboardSearchConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseSearchableClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.BatchGetEntityRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;

public class Dashboards extends BaseSearchableClient<Dashboard> {

    private static final DashboardsRequestBuilders DASHBOARDS_REQUEST_BUILDERS = new DashboardsRequestBuilders();
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

    @Nonnull
    public Map<DashboardUrn, Dashboard> batchGet(@Nonnull Set<DashboardUrn> urns)
            throws RemoteInvocationException {
        BatchGetEntityRequest<ComplexResourceKey<DashboardKey, EmptyRecord>, Dashboard> batchGetRequest
                = DASHBOARDS_REQUEST_BUILDERS.batchGet()
                .ids(urns.stream().map(this::getKeyFromUrn).collect(Collectors.toSet()))
                .build();

        return _client.sendRequest(batchGetRequest).getResponseEntity().getResults()
                .entrySet().stream().collect(Collectors.toMap(
                        entry -> getUrnFromKey(entry.getKey()),
                        entry -> entry.getValue().getEntity())
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
