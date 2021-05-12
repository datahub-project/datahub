package com.linkedin.glossary.client;

import com.linkedin.BatchGetUtils;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.glossary.GlossaryTerm;
import com.linkedin.glossary.GlossaryTermKey;
import com.linkedin.glossary.GlossaryTermsFindBySearchRequestBuilder;
import com.linkedin.glossary.GlossaryTermsRequestBuilders;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseBrowsableClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetAllRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.glossary.GlossaryTermsDoAutocompleteRequestBuilder;
import com.linkedin.glossary.GlossaryTermsDoBrowseRequestBuilder;
import com.linkedin.glossary.GlossaryTermsDoGetBrowsePathsRequestBuilder;
import com.linkedin.metadata.query.BrowseResult;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.dao.utils.QueryUtils.*;

public class GlossaryTerms extends BaseBrowsableClient<GlossaryTerm, GlossaryTermUrn> {

  private static final GlossaryTermsRequestBuilders BUSINESS_TERMS_REQUEST_BUILDERS = new GlossaryTermsRequestBuilders();

  public GlossaryTerms(@Nonnull Client restliClient) {
    super(restliClient);
  }

  /**
   * Gets {@link GlossaryTerm} model of the corp user
   *
   * @param urn corp user urn
   * @return {@link GlossaryTerm} model of the corp user
   * @throws RemoteInvocationException
   */
  @Nonnull
  public GlossaryTerm get(@Nonnull GlossaryTermUrn urn)
      throws RemoteInvocationException {
    GetRequest<GlossaryTerm> getRequest = BUSINESS_TERMS_REQUEST_BUILDERS.get()
        .id(new ComplexResourceKey<>(toGlossaryTermKey(urn), new EmptyRecord()))
        .build();

    return _client.sendRequest(getRequest).getResponse().getEntity();
  }

  /**
   * Batch gets list of {@link GlossaryTerm} models
   *
   * @param urns list of dataset urn
   * @return map of {@link Dataset} models
   * @throws RemoteInvocationException
   */
  @Nonnull
  public Map<GlossaryTermUrn, GlossaryTerm> batchGet(@Nonnull Set<GlossaryTermUrn> urns)
          throws RemoteInvocationException {
    return BatchGetUtils.batchGet(
            urns,
            (Void v) -> BUSINESS_TERMS_REQUEST_BUILDERS.batchGet(),
            this::getKeyFromUrn,
            this::getUrnFromKey,
            _client
    );
  }

  /**
   * Get all {@link GlossaryTerm} models of the corp users
   *
   * @param start offset to start
   * @param count number of max {@link GlossaryTerm}s to return
   * @return {@link GlossaryTerm} models of the corp user
   * @throws RemoteInvocationException
   */
  @Nonnull
  public List<GlossaryTerm> getAll(int start, int count)
      throws RemoteInvocationException {
    final GetAllRequest<GlossaryTerm> getAllRequest = BUSINESS_TERMS_REQUEST_BUILDERS.getAll()
        .paginate(start, count)
        .build();
    return _client.sendRequest(getAllRequest).getResponseEntity().getElements();
  }

  /**
   * Get all {@link GlossaryTerm} models of the corp users
   *
   * @return {@link GlossaryTerm} models of the corp user
   * @throws RemoteInvocationException
   */
  @Nonnull
  public List<GlossaryTerm> getAll()
      throws RemoteInvocationException {
    GetAllRequest<GlossaryTerm> getAllRequest = BUSINESS_TERMS_REQUEST_BUILDERS.getAll()
        .paginate(0, 10000)
        .build();
    return _client.sendRequest(getAllRequest).getResponseEntity().getElements();
  }

  @Override
  @Nonnull
  public CollectionResponse<GlossaryTerm> search(@Nonnull String input, @Nullable StringArray aspectNames,
                                             @Nullable Map<String, String> requestFilters, @Nullable SortCriterion sortCriterion, int start, int count)
          throws RemoteInvocationException {
    final Filter filter = (requestFilters != null) ? newFilter(requestFilters) : null;
    final GlossaryTermsFindBySearchRequestBuilder requestBuilder = BUSINESS_TERMS_REQUEST_BUILDERS.findBySearch()
            .inputParam(input)
            .aspectsParam(aspectNames)
            .filterParam(filter)
            .sortParam(sortCriterion)
            .paginate(start, count);
    return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
  }

  @Nonnull
  public CollectionResponse<GlossaryTerm> search(@Nonnull String input, @Nullable Map<String, String> requestFilters,
                                             int start, int count) throws RemoteInvocationException {
    return search(input, requestFilters, null, start, count);
  }

  @Nonnull
  public CollectionResponse<GlossaryTerm> search(@Nonnull String input, int start, int count)
          throws RemoteInvocationException {
    return search(input, null, null, start, count);
  }

  /**
   * Auto complete glossary terms
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
    GlossaryTermsDoAutocompleteRequestBuilder requestBuilder = BUSINESS_TERMS_REQUEST_BUILDERS
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
    GlossaryTermsDoBrowseRequestBuilder requestBuilder = BUSINESS_TERMS_REQUEST_BUILDERS
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
   * Gets browse path(s) given glossary term urn
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException
   */
  @Nonnull
  public StringArray getBrowsePaths(@Nonnull GlossaryTermUrn urn) throws RemoteInvocationException {
    GlossaryTermsDoGetBrowsePathsRequestBuilder requestBuilder = BUSINESS_TERMS_REQUEST_BUILDERS
            .actionGetBrowsePaths()
            .urnParam(urn);
    return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
  }

  @Nonnull
  private GlossaryTermKey toGlossaryTermKey(@Nonnull GlossaryTermUrn urn) {
    return new GlossaryTermKey()
            .setName(urn.getNameEntity());
  }

  @Nonnull
  protected GlossaryTermUrn toGlossaryTermUrn(@Nonnull GlossaryTermKey key) {
    return new GlossaryTermUrn(key.getName());
  }

  @Nonnull
  private ComplexResourceKey<GlossaryTermKey, EmptyRecord> getKeyFromUrn(@Nonnull GlossaryTermUrn urn) {
    return new ComplexResourceKey<>(toGlossaryTermKey(urn), new EmptyRecord());
  }

  @Nonnull
  private GlossaryTermUrn getUrnFromKey(@Nonnull ComplexResourceKey<GlossaryTermKey, EmptyRecord> key) {
    return toGlossaryTermUrn(key.getKey());
  }
}
