package com.linkedin.identity.client;

import com.linkedin.common.client.CorpUsersClient;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.*;
import com.linkedin.identity.corpuser.SnapshotRequestBuilders;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.SnapshotKey;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.*;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;

public class CorpUsers extends CorpUsersClient {

    private static final CorpUsersRequestBuilders CORP_USERS_REQUEST_BUILDERS = new CorpUsersRequestBuilders();
    private static final EditableInfoRequestBuilders EDITABLE_INFO_REQUEST_BUILDERS = new EditableInfoRequestBuilders();
    private static final SnapshotRequestBuilders SNAPSHOT_REQUEST_BUILDERS = new SnapshotRequestBuilders();

    public CorpUsers(@Nonnull Client restliClient) {
        super(restliClient);
    }

    /**
     * Gets {@link CorpUser} model of the corp user
     *
     * @param urn corp user urn
     * @return {@link CorpUser} model of the corp user
     * @throws RemoteInvocationException
     */
    @Nonnull
    public CorpUser get(@Nonnull CorpuserUrn urn)
            throws RemoteInvocationException {
        GetRequest<CorpUser> getRequest = CORP_USERS_REQUEST_BUILDERS.get()
                .id(new ComplexResourceKey<>(toCorpUserKey(urn), new EmptyRecord()))
                .build();

        return _client.sendRequest(getRequest).getResponse().getEntity();
    }

    /**
     * Get all {@link CorpUser} models of the corp users
     *
     * @return {@link CorpUser} models of the corp user
     * @throws RemoteInvocationException
     */
    @Nonnull
    public List<CorpUser> getAll()
            throws RemoteInvocationException {
        GetAllRequest<CorpUser> getAllRequest = CORP_USERS_REQUEST_BUILDERS.getAll()
                .build();
        return _client.sendRequest(getAllRequest).getResponseEntity().getElements();
    }

    /**
     * Batch gets list of {@link CorpUser} models of the corp users
     *
     * @param urns list of corp user urn
     * @return map of {@link CorpUser} models of the corp users
     * @throws RemoteInvocationException
     */
    @Nonnull
    public Map<CorpuserUrn, CorpUser> batchGet(@Nonnull Set<CorpuserUrn> urns)
        throws RemoteInvocationException {
        BatchGetEntityRequest<ComplexResourceKey<CorpUserKey, EmptyRecord>, CorpUser> batchGetRequest
            = CORP_USERS_REQUEST_BUILDERS.batchGet()
            .ids(urns.stream().map(this::getKeyFromUrn).collect(Collectors.toSet()))
            .build();

        return _client.sendRequest(batchGetRequest).getResponseEntity().getResults()
            .entrySet().stream().collect(Collectors.toMap(
                entry -> getUrnFromKey(entry.getKey()),
                entry -> entry.getValue().getEntity())
            );
    }

    /**
     * Adds {@link CorpUserSnapshot} to {@link CorpuserUrn}
     *
     * @param urn corp user urn
     * @param snapshot {@link CorpUserSnapshot}
     * @return Snapshot key
     * @throws RemoteInvocationException
     */
    @Nonnull
    public SnapshotKey create(@Nonnull CorpuserUrn urn, @Nonnull CorpUserSnapshot snapshot)
            throws RemoteInvocationException {
        CreateIdRequest<ComplexResourceKey<SnapshotKey, EmptyRecord>, CorpUserSnapshot> createRequest =
                SNAPSHOT_REQUEST_BUILDERS.create()
                        .corpUserKey(getKeyFromUrn(urn))
                        .input(snapshot)
                        .build();

        return _client.sendRequest(createRequest).getResponseEntity().getId().getKey();
    }

    /**
     * Creates {@link CorpUserEditableInfo} aspect
     *
     * @param corpuserUrn corp user urn
     * @param corpUserEditableInfo {@link CorpUserEditableInfo} object
     */
    public void createEditableInfo(@Nonnull CorpuserUrn corpuserUrn,
                                   @Nonnull CorpUserEditableInfo corpUserEditableInfo) throws RemoteInvocationException {
        CreateIdRequest<Long, CorpUserEditableInfo> request = EDITABLE_INFO_REQUEST_BUILDERS.create()
                .corpUserKey(getKeyFromUrn(corpuserUrn))
                .input(corpUserEditableInfo)
                .build();
        _client.sendRequest(request).getResponse();
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
    public CollectionResponse<CorpUser> search(@Nonnull String input, @Nonnull Map<String, String> requestFilters,
                                               int start, int count) throws RemoteInvocationException {

        CorpUsersFindBySearchRequestBuilder requestBuilder = CORP_USERS_REQUEST_BUILDERS
                .findBySearch()
                .inputParam(input)
                .filterParam(newFilter(requestFilters))
                .paginate(start, count);
        return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
    }

    @Nonnull
    private ComplexResourceKey<CorpUserKey, EmptyRecord> getKeyFromUrn(@Nonnull CorpuserUrn urn) {
        return new ComplexResourceKey<>(toCorpUserKey(urn), new EmptyRecord());
    }

    @Nonnull
    private CorpuserUrn getUrnFromKey(@Nonnull ComplexResourceKey<CorpUserKey, EmptyRecord> key) {
        return toCorpUserUrn(key.getKey());
    }
}