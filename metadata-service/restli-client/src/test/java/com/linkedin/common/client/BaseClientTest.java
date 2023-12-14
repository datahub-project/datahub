package com.linkedin.common.client;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.data.template.RequiredFieldNotPresentException;
import com.linkedin.entity.AspectsDoIngestProposalRequestBuilder;
import com.linkedin.entity.AspectsRequestBuilders;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.ActionRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.ResponseFuture;
import org.testng.annotations.Test;

public class BaseClientTest {
  static final Authentication AUTH =
      new Authentication(new Actor(ActorType.USER, "fake"), "foo:bar");

  @Test
  public void testZeroRetry() throws RemoteInvocationException {
    MetadataChangeProposal mcp = new MetadataChangeProposal();

    AspectsDoIngestProposalRequestBuilder testRequestBuilder =
        new AspectsRequestBuilders().actionIngestProposal().proposalParam(mcp).asyncParam("false");
    Client mockRestliClient = mock(Client.class);
    ResponseFuture<String> mockFuture = mock(ResponseFuture.class);
    when(mockRestliClient.sendRequest(any(ActionRequest.class))).thenReturn(mockFuture);

    RestliEntityClient testClient =
        new RestliEntityClient(mockRestliClient, new ExponentialBackoff(1), 0);
    testClient.sendClientRequest(testRequestBuilder, AUTH);
    // Expected 1 actual try and 0 retries
    verify(mockRestliClient).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testMultipleRetries() throws RemoteInvocationException {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    AspectsDoIngestProposalRequestBuilder testRequestBuilder =
        new AspectsRequestBuilders().actionIngestProposal().proposalParam(mcp).asyncParam("false");
    Client mockRestliClient = mock(Client.class);
    ResponseFuture<String> mockFuture = mock(ResponseFuture.class);

    when(mockRestliClient.sendRequest(any(ActionRequest.class)))
        .thenThrow(new RuntimeException())
        .thenReturn(mockFuture);

    RestliEntityClient testClient =
        new RestliEntityClient(mockRestliClient, new ExponentialBackoff(1), 1);
    testClient.sendClientRequest(testRequestBuilder, AUTH);
    // Expected 1 actual try and 1 retries
    verify(mockRestliClient, times(2)).sendRequest(any(ActionRequest.class));
  }

  @Test
  public void testNonRetry() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    AspectsDoIngestProposalRequestBuilder testRequestBuilder =
        new AspectsRequestBuilders().actionIngestProposal().proposalParam(mcp).asyncParam("false");
    Client mockRestliClient = mock(Client.class);

    when(mockRestliClient.sendRequest(any(ActionRequest.class)))
        .thenThrow(new RuntimeException(new RequiredFieldNotPresentException("value")));

    RestliEntityClient testClient =
        new RestliEntityClient(mockRestliClient, new ExponentialBackoff(1), 1);
    assertThrows(
        RuntimeException.class, () -> testClient.sendClientRequest(testRequestBuilder, AUTH));
  }
}
