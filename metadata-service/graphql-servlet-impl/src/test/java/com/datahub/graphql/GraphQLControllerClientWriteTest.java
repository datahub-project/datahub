package com.datahub.graphql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.gms.factory.clientwrite.ClientWrite;
import com.linkedin.gms.factory.clientwrite.ClientWriteRecorder;
import com.linkedin.metadata.config.ClientWriteTrackingConfiguration;
import com.linkedin.metadata.ratelimit.GraphqlDocumentAnalyzer;
import jakarta.servlet.http.HttpServletRequest;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphQLControllerClientWriteTest {

  private static final String MUTATION =
      "mutation createTag($input: CreateTagInput!) { createTag(input: $input) }";
  private static final String QUERY = "query getTag($urn: String!) { tag(urn: $urn) { urn } }";
  private static final Authentication AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, "svc_sdk"), "test");

  private GraphQLController controller;
  private ClientWriteRecorder recorder;

  @BeforeMethod
  public void setUp() {
    controller = new GraphQLController();
    recorder = mock(ClientWriteRecorder.class);
    controller.clientWriteRecorder = recorder;
    controller.clientWriteTrackingConfiguration = new ClientWriteTrackingConfiguration();
  }

  private static HttpServletRequest pythonSdkRequest() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader("X-DataHub-Py-Cli-Version")).thenReturn("1.7.0");
    when(request.getHeader("X-DataHub-Client-Mode")).thenReturn("SDK");
    when(request.getHeader("User-Agent")).thenReturn("DataHub-Client/1.0 (sdk; sdk; 1.7.0)");
    return request;
  }

  private void post(HttpServletRequest request, String operationName, String query) {
    controller.recordClientWrite(
        request, GraphqlDocumentAnalyzer.analyze(operationName, query), AUTHENTICATION, false);
  }

  @Test
  public void testPythonSdkMutationIsCounted() {
    post(pythonSdkRequest(), "createTag", MUTATION);

    ArgumentCaptor<ClientWrite> write = ArgumentCaptor.forClass(ClientWrite.class);
    verify(recorder).record(write.capture(), eq("urn:li:corpuser:svc_sdk"), eq(false));
    assertEquals(write.getValue().getApi(), "graphql");
    assertEquals(write.getValue().getClient(), "python");
    assertEquals(write.getValue().getClientVersion(), "1.7.0");
  }

  @Test
  public void testPythonSdkQueryIsNotCounted() {
    post(pythonSdkRequest(), "getTag", QUERY);
    post(pythonSdkRequest(), null, "{ me { corpUser { urn } } }");

    verify(recorder, never()).record(any(), any(), anyBoolean());
  }

  @Test
  public void testBrowserMutationIsNotCounted() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader("X-DataHub-Request-Source")).thenReturn("browser");
    when(request.getHeader("User-Agent")).thenReturn("Mozilla/5.0");

    post(request, "createTag", MUTATION);

    verify(recorder, never()).record(any(), any(), anyBoolean());
  }

  @Test
  public void testNothingIsCountedWhenTrackingIsDisabled() {
    controller.clientWriteTrackingConfiguration.setEnabled(false);

    post(pythonSdkRequest(), "createTag", MUTATION);

    verify(recorder, never()).record(any(), any(), anyBoolean());
  }
}
