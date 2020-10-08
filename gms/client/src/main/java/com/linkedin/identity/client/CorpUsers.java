package com.linkedin.identity.client;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.identity.CorpUser;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserKey;
import com.linkedin.identity.CorpUsersDoAutocompleteRequestBuilder;
import com.linkedin.identity.CorpUsersFindBySearchRequestBuilder;
import com.linkedin.identity.CorpUsersRequestBuilders;
import com.linkedin.identity.EditableInfoRequestBuilders;
import com.linkedin.metadata.configs.CorpUserSearchConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseSearchableClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.BatchGetEntityRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.GetAllRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.QueryUtils.*;

public class CorpUsers extends BaseSearchableClient<CorpUser> {

  private static final CorpUsersRequestBuilders CORP_USERS_REQUEST_BUILDERS = new CorpUsersRequestBuilders();
  private static final EditableInfoRequestBuilders EDITABLE_INFO_REQUEST_BUILDERS = new EditableInfoRequestBuilders();
  private static final CorpUserSearchConfig CORP_USER_SEARCH_CONFIG = new CorpUserSearchConfig();

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
   * Get all {@link CorpUser} models of the corp users
   *
   * @param start offset to start
   * @param count number of max {@link CorpUser}s to return
   * @return {@link CorpUser} models of the corp user
   * @throws RemoteInvocationException
   */
  @Nonnull
  public List<CorpUser> getAll(int start, int count)
      throws RemoteInvocationException {
    final GetAllRequest<CorpUser> getAllRequest = CORP_USERS_REQUEST_BUILDERS.getAll()
        .paginate(start, count)
        .build();
    return _client.sendRequest(getAllRequest).getResponseEntity().getElements();
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
        .paginate(0, 10000)
        .build();
    return _client.sendRequest(getAllRequest).getResponseEntity().getElements();
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
        .corpUserKey(new ComplexResourceKey<>(toCorpUserKey(corpuserUrn), new EmptyRecord()))
        .input(corpUserEditableInfo)
        .build();
    _client.sendRequest(request).getResponse();
  }

  @Override
  @Nonnull
  public CollectionResponse<CorpUser> search(@Nonnull String input, @Nullable StringArray aspectNames,
      @Nullable Map<String, String> requestFilters, @Nullable SortCriterion sortCriterion, int start, int count)
      throws RemoteInvocationException {
    final Filter filter = (requestFilters != null) ? newFilter(requestFilters) : null;
    final CorpUsersFindBySearchRequestBuilder requestBuilder = CORP_USERS_REQUEST_BUILDERS.findBySearch()
        .inputParam(input)
        .aspectsParam(aspectNames)
        .filterParam(filter)
        .sortParam(sortCriterion)
        .paginate(start, count);
    return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
  }

  @Nonnull
  public CollectionResponse<CorpUser> search(@Nonnull String input, @Nullable Map<String, String> requestFilters,
      int start, int count) throws RemoteInvocationException {
    return search(input, requestFilters, null, start, count);
  }

  @Nonnull
  public CollectionResponse<CorpUser> search(@Nonnull String input, int start, int count)
      throws RemoteInvocationException {
    return search(input, null, null, start, count);
  }

  @Nonnull
  public AutoCompleteResult autocomplete(@Nonnull String query, @Nullable String field,
      @Nullable Map<String, String> requestFilters, int limit) throws RemoteInvocationException {
    final String autocompleteField = (field != null) ? field : CORP_USER_SEARCH_CONFIG.getDefaultAutocompleteField();
    final Filter filter = (requestFilters != null) ? newFilter(requestFilters) : null;
    CorpUsersDoAutocompleteRequestBuilder requestBuilder = CORP_USERS_REQUEST_BUILDERS
        .actionAutocomplete()
        .queryParam(query)
        .fieldParam(autocompleteField)
        .filterParam(filter)
        .limitParam(limit);
    return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
  }

  @Nonnull
  private CorpUserKey toCorpUserKey(@Nonnull CorpuserUrn urn) {
    return new CorpUserKey().setName(urn.getUsernameEntity());
  }

  @Nonnull
  protected CorpuserUrn toCorpUserUrn(@Nonnull CorpUserKey key) {
    return new CorpuserUrn(key.getName());
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
