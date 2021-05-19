package com.linkedin.entity.client;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetsDoGetBrowsePathsRequestBuilder;
import com.linkedin.entity.EntitiesDoAutocompleteRequestBuilder;
import com.linkedin.entity.EntitiesDoBrowseRequestBuilder;
import com.linkedin.entity.EntitiesDoGetBrowsePathsRequestBuilder;
import com.linkedin.entity.EntitiesDoSearchRequestBuilder;
import com.linkedin.entity.EntitiesRequestBuilders;
import com.linkedin.experimental.Entity;
import com.linkedin.metadata.dao.RemoteEntityDao;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.BatchGetEntityRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetRequest;

import javax.annotation.Nonnull;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.QueryUtils.*;


public class EntityClient {

    private static final EntitiesRequestBuilders ENTITIES_REQUEST_BUILDERS = new EntitiesRequestBuilders();

    private final Client _client;
    private final RemoteEntityDao _writeDao;

    public EntityClient(@Nonnull final Client restliClient) {
        _client = restliClient;
        _writeDao = new RemoteEntityDao(restliClient);
    }

    @Nonnull
    public RecordTemplate get(@Nonnull final Urn urn) throws RemoteInvocationException {
        final GetRequest<Entity> getRequest = ENTITIES_REQUEST_BUILDERS.get()
                .id(urn.toString())
                .build();
        return _client.sendRequest(getRequest).getResponse().getEntity();
    }

    @Nonnull
    public Map<Urn, Entity> batchGet(@Nonnull final Set<Urn> urns) throws RemoteInvocationException {

        final Integer batchSize = 25;
        final AtomicInteger index = new AtomicInteger(0);

        final Collection<List<Urn>> entityUrnBatches = urns.stream()
                .collect(Collectors.groupingBy(x -> index.getAndIncrement() / batchSize))
                .values();

        final Map<Urn, Entity> response = new HashMap<>();

        for (List<Urn> urnsInBatch : entityUrnBatches) {
            BatchGetEntityRequest<String, Entity> batchGetRequest =
                    ENTITIES_REQUEST_BUILDERS.batchGet()
                            .ids(urnsInBatch.stream().map(urn -> urn.toString()).collect(Collectors.toSet()))
                            .build();
            final Map<Urn, Entity> batchResponse = _client.sendRequest(batchGetRequest).getResponseEntity().getResults()
                    .entrySet().stream().collect(Collectors.toMap(
                            entry -> {
                                try {
                                    return Urn.createFromString(entry.getKey());
                                } catch (URISyntaxException e) {
                                   throw new RuntimeException(String.format("Failed to create Urn from key string %s", entry.getKey()));
                                }
                            },
                            entry -> entry.getValue().getEntity())
                    );
            response.putAll(batchResponse);
        }
        return response;
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
    public AutoCompleteResult autoComplete(@Nonnull String entityType, @Nonnull String query, @Nonnull String field,
        @Nonnull Map<String, String> requestFilters,
        @Nonnull int limit) throws RemoteInvocationException {
        EntitiesDoAutocompleteRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS
            .actionAutocomplete()
            .entityParam(entityType)
            .queryParam(query)
            .fieldParam(field)
            .filterParam(newFilter(requestFilters))
            .limitParam(limit);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Gets browse snapshot of a given path
     *
     * @param entityType entity type being browse
     * @param path path being browsed
     * @param requestFilters browse filters
     * @param start start offset of first dataset
     * @param limit max number of datasets
     * @throws RemoteInvocationException
     */
    @Nonnull
    public BrowseResult browse(@Nonnull String entityType, @Nonnull String path, @Nullable Map<String, String> requestFilters,
        int start, int limit) throws RemoteInvocationException {
        EntitiesDoBrowseRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS
            .actionBrowse()
            .pathParam(path)
            .entityParam(entityType)
            .startParam(start)
            .limitParam(limit);
        if (requestFilters != null) {
            requestBuilder.filterParam(newFilter(requestFilters));
        }
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    public void update(@Nonnull final Entity entity) throws RemoteInvocationException {
        // TODO: Consolidate Dao and Client --> Why have 2?
        _writeDao.create(entity);
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
    public SearchResult search(@Nonnull String entity, @Nonnull String input, @Nonnull Map<String, String> requestFilters,
        int start, int count) throws RemoteInvocationException {

        return search(entity, input, null, requestFilters, start, count);
    }

    @Nonnull
    public SearchResult search(@Nonnull String entity, @Nonnull String input, @Nullable StringArray aspectNames,
        @Nullable Map<String, String> requestFilters, int start, int count)
        throws RemoteInvocationException {

        final EntitiesDoSearchRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionSearch()
            .entityParam(entity)
            .inputParam(input)
            .filterParam(newFilter(requestFilters))
            .startParam(start)
            .countParam(count);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    /**
     * Gets browse path(s) given dataset urn
     *
     * @param urn urn for the entity
     * @return list of paths given urn
     * @throws RemoteInvocationException
     */
    @Nonnull
    public StringArray getBrowsePaths(@Nonnull Urn urn) throws RemoteInvocationException {
        EntitiesDoGetBrowsePathsRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS
            .actionGetBrowsePaths()
            .urnParam(urn);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }
}
