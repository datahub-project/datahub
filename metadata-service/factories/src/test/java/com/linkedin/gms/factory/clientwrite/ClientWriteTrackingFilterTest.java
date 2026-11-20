package com.linkedin.gms.factory.clientwrite;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class ClientWriteTrackingFilterTest {

  private static final String SYSTEM_ACTOR = "urn:li:corpuser:__datahub_system";

  @AfterMethod
  public void tearDown() {
    AuthenticationContext.remove();
  }

  private static MockHttpServletRequest ingestRequest() {
    MockHttpServletRequest request = new MockHttpServletRequest("POST", "/gms/aspects");
    request.setQueryString("action=ingestProposal");
    request.addHeader("X-DataHub-Py-Cli-Version", "1.8.0");
    return request;
  }

  @Test
  public void testRecordsWriteWithAuthenticatedActor() throws Exception {
    AuthenticationContext.setAuthentication(
        new Authentication(new Actor(ActorType.USER, "svc_loader"), ""));
    ClientWriteRecorder recorder = mock(ClientWriteRecorder.class);
    MockFilterChain chain = new MockFilterChain();

    new ClientWriteTrackingFilter(recorder, "/gms", SYSTEM_ACTOR)
        .doFilter(ingestRequest(), new MockHttpServletResponse(), chain);

    verify(recorder).record(any(ClientWrite.class), eq("urn:li:corpuser:svc_loader"), eq(false));
    assertNotNull(chain.getRequest(), "the request must continue down the chain");
  }

  @Test
  public void testSkipsNonWrites() throws Exception {
    ClientWriteRecorder recorder = mock(ClientWriteRecorder.class);
    new ClientWriteTrackingFilter(recorder, null, SYSTEM_ACTOR)
        .doFilter(
            new MockHttpServletRequest("GET", "/openapi/v3/entity/dataset"),
            new MockHttpServletResponse(),
            new MockFilterChain());
    verifyNoInteractions(recorder);
  }

  @Test
  public void testRecorderFailureNeverBlocksTheRequest() throws Exception {
    ClientWriteRecorder recorder = mock(ClientWriteRecorder.class);
    doThrow(new IllegalStateException("boom")).when(recorder).record(any(), any(), eq(false));
    MockFilterChain chain = new MockFilterChain();

    new ClientWriteTrackingFilter(recorder, "/gms", SYSTEM_ACTOR)
        .doFilter(ingestRequest(), new MockHttpServletResponse(), chain);

    assertNotNull(chain.getRequest(), "the request must continue down the chain");
  }
}
