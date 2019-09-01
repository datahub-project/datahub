package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import javax.cache.Cache;
import org.testng.annotations.BeforeMethod;

import static org.mockito.Mockito.*;


public class RestliRemoteDAOTest {

  private RestClient _mockRestClient;
  private Cache<AspectKey, RecordTemplate> _mockCache;

  @BeforeMethod
  public void setup() {
    _mockRestClient = mock(RestClient.class);
    _mockCache = mock(Cache.class);
  }

  private <SNAPSHOT extends RecordTemplate> void expectGetRequest(SNAPSHOT snapshot) throws RemoteInvocationException {
    Response<SNAPSHOT> response = mock(Response.class);
    when(response.getEntity()).thenReturn(snapshot);
    ResponseFuture<SNAPSHOT> future = mock(ResponseFuture.class);
    when(future.getResponse()).thenReturn(response);

    when(_mockRestClient.sendRequest(any(GetRequest.class))).thenReturn(future);
  }
}
