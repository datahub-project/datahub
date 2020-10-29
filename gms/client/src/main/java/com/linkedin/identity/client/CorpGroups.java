package com.linkedin.identity.client;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.identity.CorpGroup;
import com.linkedin.identity.CorpGroupKey;
import com.linkedin.identity.CorpGroupsDoAutocompleteRequestBuilder;
import com.linkedin.identity.CorpGroupsFindBySearchRequestBuilder;
import com.linkedin.identity.CorpGroupsRequestBuilders;
import com.linkedin.metadata.configs.CorpGroupSearchConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseSearchableClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.BatchGetEntityRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.QueryUtils.*;

public class CorpGroups extends BaseSearchableClient<CorpGroup> {
  private static final CorpGroupsRequestBuilders CORP_GROUPS_REQUEST_BUILDERS = new CorpGroupsRequestBuilders();
  private static final CorpGroupSearchConfig CORP_GROUP_SEARCH_CONFIG = new CorpGroupSearchConfig();

  public CorpGroups(@Nonnull Client restliClient) {
    super(restliClient);
  }

  /**
   * Gets {@link CorpGroup} model of the corp group
   *
   * @param urn corp group urn
   * @return {@link CorpGroup} model of the corp group
   * @throws RemoteInvocationException
   */
  @Nonnull
  public CorpGroup get(@Nonnull CorpGroupUrn urn)
      throws RemoteInvocationException {
    GetRequest<CorpGroup> getRequest = CORP_GROUPS_REQUEST_BUILDERS.get()
        .id(new ComplexResourceKey<>(toCorpGroupKey(urn), new EmptyRecord()))
        .build();

    return _client.sendRequest(getRequest).getResponse().getEntity();
  }

  /**
   * Batch gets list of {@link CorpGroup} models of the corp groups
   *
   * @param urns list of corp group urn
   * @return map of {@link CorpGroup} models of the corp groups
   * @throws RemoteInvocationException
   */
  @Nonnull
  public Map<CorpGroupUrn, CorpGroup> batchGet(@Nonnull Set<CorpGroupUrn> urns)
      throws RemoteInvocationException {
    BatchGetEntityRequest<ComplexResourceKey<CorpGroupKey, EmptyRecord>, CorpGroup> batchGetRequest
        = CORP_GROUPS_REQUEST_BUILDERS.batchGet()
        .ids(urns.stream().map(this::getKeyFromUrn).collect(Collectors.toSet()))
        .build();

    return _client.sendRequest(batchGetRequest).getResponseEntity().getResults()
        .entrySet().stream().collect(Collectors.toMap(
            entry -> getUrnFromKey(entry.getKey()),
            entry -> entry.getValue().getEntity())
        );
  }

  @Override
  @Nonnull
  public CollectionResponse<CorpGroup> search(@Nonnull String input, @Nullable StringArray aspectNames,
      @Nullable Map<String, String> requestFilters, @Nullable SortCriterion sortCriterion, int start, int count)
      throws RemoteInvocationException {
    final Filter filter = (requestFilters != null) ? newFilter(requestFilters) : null;
    final CorpGroupsFindBySearchRequestBuilder requestBuilder = CORP_GROUPS_REQUEST_BUILDERS.findBySearch()
        .inputParam(input)
        .aspectsParam(aspectNames)
        .filterParam(filter)
        .sortParam(sortCriterion)
        .paginate(start, count);
    return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
  }

  @Nonnull
  public CollectionResponse<CorpGroup> search(@Nonnull String input, @Nullable Map<String, String> requestFilters,
      int start, int count) throws RemoteInvocationException {
    return search(input, requestFilters, null, start, count);
  }

  @Nonnull
  public CollectionResponse<CorpGroup> search(@Nonnull String input, int start, int count)
      throws RemoteInvocationException {
    return search(input, null, null, start, count);
  }

  @Nonnull
  public AutoCompleteResult autocomplete(@Nonnull String query, @Nullable String field,
      @Nullable Map<String, String> requestFilters, int limit) throws RemoteInvocationException {
    final String autocompleteField = (field != null) ? field : CORP_GROUP_SEARCH_CONFIG.getDefaultAutocompleteField();
    final Filter filter = (requestFilters != null) ? newFilter(requestFilters) : null;
    CorpGroupsDoAutocompleteRequestBuilder requestBuilder = CORP_GROUPS_REQUEST_BUILDERS
        .actionAutocomplete()
        .queryParam(query)
        .fieldParam(autocompleteField)
        .filterParam(filter)
        .limitParam(limit);
    return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
  }

  @Nonnull
  private CorpGroupKey toCorpGroupKey(@Nonnull CorpGroupUrn urn) {
    return new CorpGroupKey().setName(urn.getGroupNameEntity());
  }

  @Nonnull
  private ComplexResourceKey<CorpGroupKey, EmptyRecord> getKeyFromUrn(@Nonnull CorpGroupUrn urn) {
    return new ComplexResourceKey<>(toCorpGroupKey(urn), new EmptyRecord());
  }

  @Nonnull
  private CorpGroupUrn toCorpGroupUrn(@Nonnull CorpGroupKey key) {
    return new CorpGroupUrn(key.getName());
  }

  @Nonnull
  private CorpGroupUrn getUrnFromKey(@Nonnull ComplexResourceKey<CorpGroupKey, EmptyRecord> key) {
    return toCorpGroupUrn(key.getKey());
  }
}