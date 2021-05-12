package com.linkedin.entity.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EntitiesRequestBuilders;
import com.linkedin.experimental.Entity;
import com.linkedin.metadata.dao.RemoteEntityDao;
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

    public void update(@Nonnull final Entity entity) throws RemoteInvocationException {
        // TODO: Consolidate Dao and Client --> Why have 2?
        _writeDao.create(entity);
    }
    // TODO: Add Search, Browse here also.
}

