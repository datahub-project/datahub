package com.linkedin.dataprocess.client;

import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.dataprocess.DataProcess;
import com.linkedin.dataprocess.DataProcessInfoRequestBuilders;
import com.linkedin.dataprocess.DataProcessesRequestBuilders;
import com.linkedin.dataprocess.DataProcessKey;
import com.linkedin.dataprocess.DataProcessInfo;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseClient;
import com.linkedin.metadata.restli.SearchableClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.BatchGetEntityRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class DataProcesses extends BaseClient implements SearchableClient<DataProcess> {

  private static final DataProcessesRequestBuilders PROCESSES_REQUEST_BUILDERS = new DataProcessesRequestBuilders();
  private static final DataProcessInfoRequestBuilders PROCESS_INFO_REQUEST_BUILDERS = new DataProcessInfoRequestBuilders();

  protected DataProcesses(@Nonnull Client restliClient) {
    super(restliClient);
  }

  @Nonnull
  public DataProcess get(@Nonnull DataProcessUrn urn)
      throws RemoteInvocationException {
    GetRequest<DataProcess> getRequest = PROCESSES_REQUEST_BUILDERS.get()
        .id(new ComplexResourceKey<>(toDataProcessKey(urn), new EmptyRecord()))
        .build();

    return _client.sendRequest(getRequest).getResponse().getEntity();
  }

  @Nonnull
  public Map<DataProcessUrn, DataProcess> batchGet(@Nonnull Set<DataProcessUrn> urns)
      throws RemoteInvocationException {
    BatchGetEntityRequest<ComplexResourceKey<DataProcessKey, EmptyRecord>, DataProcess> batchGetRequest
        = PROCESSES_REQUEST_BUILDERS.batchGet()
        .ids(urns.stream().map(this::getKeyFromUrn).collect(Collectors.toSet()))
        .build();

    return _client.sendRequest(batchGetRequest).getResponseEntity().getResults()
        .entrySet().stream().collect(Collectors.toMap(
            entry -> getUrnFromKey(entry.getKey()),
            entry -> entry.getValue().getEntity())
        );
  }

  public void createDataProcessInfo(@Nonnull DataProcessUrn dataProcessUrn,
      @Nonnull DataProcessInfo dataProcessInfo) throws RemoteInvocationException {
    CreateIdRequest<Long, DataProcessInfo> request = PROCESS_INFO_REQUEST_BUILDERS.create()
        .dataprocessKey(new ComplexResourceKey<>(toDataProcessKey(dataProcessUrn), new EmptyRecord()))
        .input(dataProcessInfo)
        .build();
    _client.sendRequest(request).getResponse();
  }

  @Nonnull
  private DataProcessKey toDataProcessKey(@Nonnull DataProcessUrn urn) {
    return new DataProcessKey().setName(urn.getNameEntity());
  }

  @Nonnull
  protected DataProcessUrn toDataProcessUrn(@Nonnull DataProcessKey key) {
    return new DataProcessUrn(key.getOrchestrator(), key.getName(), key.getOrigin());
  }

  @Nonnull
  private ComplexResourceKey<DataProcessKey, EmptyRecord> getKeyFromUrn(@Nonnull DataProcessUrn urn) {
    return new ComplexResourceKey<>(toDataProcessKey(urn), new EmptyRecord());
  }

  @Nonnull
  private DataProcessUrn getUrnFromKey(@Nonnull ComplexResourceKey<DataProcessKey, EmptyRecord> key) {
    return toDataProcessUrn(key.getKey());
  }

  @Nonnull
  @Override
  public CollectionResponse<DataProcess> search(@Nonnull String input, @Nullable Map<String, String> requestFilters,
      @Nullable SortCriterion sortCriterion, int start, int count) throws RemoteInvocationException {
    throw new UnsupportedOperationException(
        String.format("%s doesn't support search feature yet,",
            this.getClass().getName())
    );
  }

  @Nonnull
  @Override
  public AutoCompleteResult autocomplete(@Nonnull String query, @Nullable String field, @Nullable Map<String, String> requestFilters, int limit)
      throws RemoteInvocationException {
    throw new UnsupportedOperationException(
        String.format("%s doesn't support auto completion feature yet,",
            this.getClass().getName())
    );
  }
}
