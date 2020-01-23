package com.linkedin.identity.client;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.identity.CorpGroupKey;
import com.linkedin.identity.corpgroup.SnapshotRequestBuilders;
import com.linkedin.metadata.restli.BaseClient;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import com.linkedin.metadata.snapshot.SnapshotKey;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import javax.annotation.Nonnull;


public class CorpGroupSnapshots extends BaseClient {

  private static final SnapshotRequestBuilders SNAPSHOT_REQUEST_BUILDERS = new SnapshotRequestBuilders();

  public CorpGroupSnapshots(@Nonnull Client restliClient) {
    super(restliClient);
  }

  @Nonnull
  public CorpGroupSnapshot get(@Nonnull CorpGroupUrn urn, @Nonnull SnapshotKey snapshotKey)
      throws RemoteInvocationException {
    GetRequest<CorpGroupSnapshot> getRequest = SNAPSHOT_REQUEST_BUILDERS.get()
        .corpGroupKey(new ComplexResourceKey<>(toCorpGroupKey(urn), new EmptyRecord()))
        .id(new ComplexResourceKey<>(snapshotKey, new EmptyRecord()))
        .build();

    return _client.sendRequest(getRequest).getResponseEntity();
  }

  @Nonnull
  public SnapshotKey create(@Nonnull CorpGroupUrn urn, @Nonnull CorpGroupSnapshot snapshot)
      throws RemoteInvocationException {
    CreateIdRequest<ComplexResourceKey<SnapshotKey, EmptyRecord>, CorpGroupSnapshot> createRequest =
        SNAPSHOT_REQUEST_BUILDERS.create()
            .corpGroupKey(new ComplexResourceKey<>(toCorpGroupKey(urn), new EmptyRecord()))
            .input(snapshot)
            .build();

    return _client.sendRequest(createRequest).getResponseEntity().getId().getKey();
  }

  private CorpGroupKey toCorpGroupKey(CorpGroupUrn urn) {
    return new CorpGroupKey().setName(urn.getGroupNameEntity());
  }
}
