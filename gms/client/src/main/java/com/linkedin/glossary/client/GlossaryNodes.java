package com.linkedin.glossary.client;

import com.linkedin.BatchGetUtils;
import com.linkedin.glossary.GlossaryNode;
import com.linkedin.glossary.GlossaryNodeKey;
import com.linkedin.glossary.GlossaryNodesFindBySearchRequestBuilder;
import com.linkedin.glossary.GlossaryNodesRequestBuilders;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseSearchableClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetAllRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;

public class GlossaryNodes extends BaseSearchableClient<GlossaryNode> {

  private static final GlossaryNodesRequestBuilders BUSINESS_NODES_REQUEST_BUILDERS = new GlossaryNodesRequestBuilders();

  public GlossaryNodes(@Nonnull Client restliClient) {
    super(restliClient);
  }

  /**
   * Gets {@link GlossaryNode} model of the business node.
   *
   * @param urn business node urn
   * @return {@link GlossaryNode} model of the corp user
   * @throws RemoteInvocationException
   */
  @Nonnull
  public GlossaryNode get(@Nonnull GlossaryNodeUrn urn)
      throws RemoteInvocationException {
    GetRequest<GlossaryNode> getRequest = BUSINESS_NODES_REQUEST_BUILDERS.get()
        .id(new ComplexResourceKey<>(toGlossaryNodeKey(urn), new EmptyRecord()))
        .build();

    return _client.sendRequest(getRequest).getResponse().getEntity();
  }

  /**
   * Batch gets list of {@link GlossaryNode} models
   *
   * @param urns list of dataset urn
   * @return map of {@link Dataset} models
   * @throws RemoteInvocationException
   */
  @Nonnull
  public Map<GlossaryNodeUrn, GlossaryNode> batchGet(@Nonnull Set<GlossaryNodeUrn> urns)
          throws RemoteInvocationException {
    return BatchGetUtils.batchGet(
            urns,
            (Void v) -> BUSINESS_NODES_REQUEST_BUILDERS.batchGet(),
            this::getKeyFromUrn,
            this::getUrnFromKey,
            _client
    );
  }

  /**
   * Get all {@link GlossaryNode} models of the business nodes
   *
   * @param start offset to start
   * @param count number of max {@link GlossaryNode}s to return
   * @return {@link GlossaryNode} models of the business nodes
   * @throws RemoteInvocationException
   */
  @Nonnull
  public List<GlossaryNode> getAll(int start, int count)
      throws RemoteInvocationException {
    final GetAllRequest<GlossaryNode> getAllRequest = BUSINESS_NODES_REQUEST_BUILDERS.getAll()
        .paginate(start, count)
        .build();
    return _client.sendRequest(getAllRequest).getResponseEntity().getElements();
  }

  /**
   * Get all {@link GlossaryNode} models of the business nodes
   *
   * @return {@link GlossaryNode} models of the business nodes
   * @throws RemoteInvocationException
   */
  @Nonnull
  public List<GlossaryNode> getAll()
      throws RemoteInvocationException {
    GetAllRequest<GlossaryNode> getAllRequest = BUSINESS_NODES_REQUEST_BUILDERS.getAll()
        .paginate(0, 10000)
        .build();
    return _client.sendRequest(getAllRequest).getResponseEntity().getElements();
  }

  @Override
  @Nonnull
  public CollectionResponse<GlossaryNode> search(@Nonnull String input, @Nullable StringArray aspectNames,
                                             @Nullable Map<String, String> requestFilters, @Nullable SortCriterion sortCriterion, int start, int count)
          throws RemoteInvocationException {
    final Filter filter = (requestFilters != null) ? newFilter(requestFilters) : null;
    final GlossaryNodesFindBySearchRequestBuilder requestBuilder = BUSINESS_NODES_REQUEST_BUILDERS.findBySearch()
            .inputParam(input)
            .aspectsParam(aspectNames)
            .filterParam(filter)
            .sortParam(sortCriterion)
            .paginate(start, count);
    return _client.sendRequest(requestBuilder.build()).getResponse().getEntity();
  }

  @Nonnull
  public CollectionResponse<GlossaryNode> search(@Nonnull String input, @Nullable Map<String, String> requestFilters,
                                             int start, int count) throws RemoteInvocationException {
    return search(input, requestFilters, null, start, count);
  }

  @Nonnull
  public CollectionResponse<GlossaryNode> search(@Nonnull String input, int start, int count)
          throws RemoteInvocationException {
    return search(input, null, null, start, count);
  }

  @Nonnull
  private GlossaryNodeKey toGlossaryNodeKey(@Nonnull GlossaryNodeUrn urn) {
    return new GlossaryNodeKey()
            .setName(urn.getNameEntity());
  }

  @Nonnull
  protected GlossaryNodeUrn toGlossaryNodeUrn(@Nonnull GlossaryNodeKey key) {
    return new GlossaryNodeUrn(key.getName());
  }

  @Nonnull
  private ComplexResourceKey<GlossaryNodeKey, EmptyRecord> getKeyFromUrn(@Nonnull GlossaryNodeUrn urn) {
    return new ComplexResourceKey<>(toGlossaryNodeKey(urn), new EmptyRecord());
  }

  @Nonnull
  private GlossaryNodeUrn getUrnFromKey(@Nonnull ComplexResourceKey<GlossaryNodeKey, EmptyRecord> key) {
    return toGlossaryNodeUrn(key.getKey());
  }
}
