package com.linkedin.metadata.dao.internal;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.RequestBuilders;
import com.linkedin.metadata.dao.RestliClientException;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import javax.annotation.Nonnull;


/**
 * A rest.li implementation of {@link BaseRemoteWriterDAO}.
 *
 * Uses rest.li snapshot endpoints to update metadata on remote services.
 */
public class RestliRemoteWriterDAO extends BaseRemoteWriterDAO {

  protected final Client _restliClient;

  public RestliRemoteWriterDAO(@Nonnull Client restliClient) {
    _restliClient = restliClient;
  }

  @Override
  public <URN extends Urn> void create(@Nonnull URN urn, @Nonnull RecordTemplate snapshot)
      throws IllegalArgumentException, RestliClientException {
    ModelUtils.validateSnapshotUrn(snapshot.getClass(), urn.getClass());

    final Request request = RequestBuilders.getBuilder(urn).createRequest(urn, snapshot);

    try {
      _restliClient.sendRequest(request).getResponse();
    } catch (RemoteInvocationException e) {
      throw new RestliClientException(e);
    }
  }
}
