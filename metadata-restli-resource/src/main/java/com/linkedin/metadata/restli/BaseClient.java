package com.linkedin.metadata.restli;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.ActionRequest;
import com.linkedin.restli.client.BatchGetEntityRequest;
import com.linkedin.restli.client.BatchGetEntityRequestBuilder;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetAllRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.base.ActionRequestBuilderBase;
import com.linkedin.restli.client.base.GetAllRequestBuilderBase;
import com.linkedin.restli.client.base.GetRequestBuilderBase;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public abstract class BaseClient implements AutoCloseable {

  protected final Client _client;

  protected BaseClient(@Nonnull Client restliClient) {
    _client = restliClient;
  }

  @Override
  public void close() {
    if (_client != null) {
      _client.shutdown(new FutureCallback<>());
    }
  }

  /**
   * Return the get result of a aspect query from a TMS/GMS endpoint. It solves the Rest.li deserialization problem
   * for the float type, which is described here: https://jira01.corp.linkedin.com:8443/browse/SI-13334
   *
   * @param request the get request
   * @param <ASPECT> the metadata aspect type
   * @return the metadata aspect of the get request.
   * @throws RemoteInvocationException when the rest.li request fails
   */
  protected <ASPECT extends RecordTemplate> ASPECT get(@Nonnull GetRequest<ASPECT> request)
      throws RemoteInvocationException {
    return _client.sendRequest(request).getResponse().getEntity();
  }

  /**
   * Similar to {@link #get(GetRequest)} but takes a @{link GetRequestBuilderBase} instead
   */
  protected <K, ASPECT extends RecordTemplate, RB extends GetRequestBuilderBase<K, ASPECT, RB>>
  ASPECT get(@Nonnull GetRequestBuilderBase<K, ASPECT, RB> requestBuilder) throws RemoteInvocationException {
    return get(requestBuilder.build());
  }

  /**
   * Return the batch get result of a aspect query from a TMS/GMS endpoint. It solves the Rest.li deserialization
   * problem for the float type, which is described here: https://jira01.corp.linkedin.com:8443/browse/SI-13334
   *
   * @param request the batch get request {@link BatchGetEntityRequest}
   * @param getUrnFunc the function to get the URN of the output from KEY of the input request
   * @param <URN> the URN that can be identified by batch get key.
   * @param <KEY> the metadata key to retrieve the metadata entity
   * @param <ASPECT> the metadata aspect type
   * @return the map of the request key to the metadata aspect of the get request.
   * @throws RemoteInvocationException when the rest.li request fails
   */
  protected <URN, KEY extends RecordTemplate, ASPECT extends RecordTemplate> Map<URN, ASPECT> batchGet(
      @Nonnull BatchGetEntityRequest<ComplexResourceKey<KEY, EmptyRecord>, ASPECT> request,
      @Nonnull Function<KEY, URN> getUrnFunc
  ) throws RemoteInvocationException {
    return _client.sendRequest(request).getResponseEntity().getResults()
        .entrySet().stream().collect(Collectors.toMap(
            entry -> getUrnFunc.apply(entry.getKey().getKey()),
            entry -> entry.getValue().getEntity()
        ));
  }

  /**
   * Similar to {@link #batchGet(BatchGetEntityRequest, Function)} but
   * takes a @{link BatchGetEntityRequestBuilder} instead
   */
  protected <URN, KEY extends RecordTemplate, ASPECT extends RecordTemplate> Map<URN, ASPECT> batchGet(
      @Nonnull BatchGetEntityRequestBuilder<ComplexResourceKey<KEY, EmptyRecord>, ASPECT> requestBuilder,
      @Nonnull Function<KEY, URN> getUrnFunc
  ) throws RemoteInvocationException {
    return batchGet(requestBuilder.build(), getUrnFunc);
  }

  /**
   * Similar to {@link #batchGet(BatchGetEntityRequest, Function)} but take default identity function.
   */
  protected <KEY extends RecordTemplate, ASPECT extends RecordTemplate> Map<KEY, ASPECT> batchGet(
      @Nonnull BatchGetEntityRequest<ComplexResourceKey<KEY, EmptyRecord>, ASPECT> request
  ) throws RemoteInvocationException {
    return batchGet(request, Function.identity());
  }

  /**
   * Similar to {@link #batchGet(BatchGetEntityRequest)} but takes a @{link BatchGetEntityRequestBuilder} instead
   */
  protected <KEY extends RecordTemplate, ASPECT extends RecordTemplate> Map<KEY, ASPECT> batchGet(
      @Nonnull BatchGetEntityRequestBuilder<ComplexResourceKey<KEY, EmptyRecord>, ASPECT> requestBuilder
  ) throws RemoteInvocationException {
    return batchGet(requestBuilder.build());
  }

  /**
   * Return the getAll result of a aspect query from a TMS/GMS endpoint. It solves the Rest.li deserialization problem
   * for the float type, which is described here: https://jira01.corp.linkedin.com:8443/browse/SI-13334
   *
   * @param request the getAll request.
   * @param <ASPECT> the metadata Aspect type
   * @return the collection of the metadata aspects of the getAll request.
   * @throws RemoteInvocationException when the rest.li request fails
   */
  protected <ASPECT extends RecordTemplate> CollectionResponse<ASPECT> getAll(@Nonnull GetAllRequest<ASPECT> request)
      throws RemoteInvocationException {
    return _client.sendRequest(request).getResponse().getEntity();
  }

  /**
   * Similar to {@link #getAll(GetAllRequest)} but takes a @{link GetAllRequestBuilderBase} instead
   */
  protected <K, ASPECT extends RecordTemplate, RB extends GetAllRequestBuilderBase<K, ASPECT, RB>>
  CollectionResponse<ASPECT> getAll(@Nonnull GetAllRequestBuilderBase<K, ASPECT, RB> requestBuilder)
      throws RemoteInvocationException {
    return getAll(requestBuilder.build());
  }

  /**
   * Return the action result of a aspect from a TMS/GMS endpoint. It solves the Rest.li deserialization
   * problem for the float type, which is described here: https://jira01.corp.linkedin.com:8443/browse/SI-13334
   *
   * @param request the rest action request.
   * @param <ASPECT> the metadata Aspect type
   * @return the metadata aspect of the request.
   * @throws RemoteInvocationException when the rest.li request fails
   */
  protected <ASPECT extends RecordTemplate> ASPECT doAction(@Nonnull ActionRequest<ASPECT> request)
      throws RemoteInvocationException {
    return _client.sendRequest(request).getResponse().getEntity();
  }

  /**
   * Similar to {@link #doAction(ActionRequest)} but takes a @{link ActionRequestBuilderBase} instead
   */
  protected <K, ASPECT extends RecordTemplate, RB extends ActionRequestBuilderBase<K, ASPECT, RB>>
  ASPECT doAction(@Nonnull ActionRequestBuilderBase<K, ASPECT, RB> requestBuilder) throws RemoteInvocationException {
    return doAction(requestBuilder.build());
  }
}
