package com.linkedin.r2.transport.http.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestStatus;
import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.transport.common.bridge.common.TransportResponse;
import com.linkedin.r2.transport.common.bridge.common.TransportResponseImpl;
import com.linkedin.r2.transport.common.bridge.server.TransportDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RAPJakartaServletTest {
  private static final int TIMEOUT_SECONDS = 5;
  private static final String REMOTE_ADDRESS = "192.168.1.1";
  private static final int REMOTE_PORT = 12345;

  private HttpDispatcher mockHttpDispatcher;
  private TransportDispatcher mockTransportDispatcher;
  private HttpServletRequest mockRequest;
  private HttpServletResponse mockResponse;
  private ServletOutputStream mockOutputStream;

  @BeforeMethod
  public void setUp() throws IOException {
    mockHttpDispatcher = mock(HttpDispatcher.class);
    mockTransportDispatcher = mock(TransportDispatcher.class);
    mockRequest = mock(HttpServletRequest.class);
    mockResponse = mock(HttpServletResponse.class);
    mockOutputStream = mock(ServletOutputStream.class);

    // Basic request setup
    when(mockRequest.getMethod()).thenReturn("GET");
    when(mockRequest.getRequestURI()).thenReturn("/test");
    when(mockRequest.getContextPath()).thenReturn("");
    when(mockRequest.getServletPath()).thenReturn("");
    when(mockRequest.getPathInfo()).thenReturn("/test");

    // Protocol and server info
    when(mockRequest.getProtocol()).thenReturn("HTTP/1.1");
    when(mockRequest.getScheme()).thenReturn("http");
    when(mockRequest.getServerName()).thenReturn("localhost");
    when(mockRequest.getServerPort()).thenReturn(8080);

    // Remote client info
    when(mockRequest.getRemoteAddr()).thenReturn(REMOTE_ADDRESS);
    when(mockRequest.getRemotePort()).thenReturn(REMOTE_PORT);

    // Security defaults (non-secure)
    when(mockRequest.isSecure()).thenReturn(false);
    when(mockRequest.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(null);
    when(mockRequest.getAttribute("javax.servlet.request.cipher_suite")).thenReturn(null);

    // Headers and content
    when(mockRequest.getHeaderNames()).thenReturn(Collections.enumeration(Collections.emptyList()));
    when(mockRequest.getInputStream())
        .thenReturn(new TestServletInputStream(new ByteArrayInputStream(new byte[0])));
    when(mockResponse.getOutputStream()).thenReturn(mockOutputStream);
  }

  @Test
  public void testConstructorWithHttpDispatcher() {
    RAPJakartaServlet servlet = new RAPJakartaServlet(mockHttpDispatcher, TIMEOUT_SECONDS);
    assertEquals(servlet.getDispatcher(), mockHttpDispatcher);
  }

  @Test
  public void testConstructorWithTransportDispatcher() {
    RAPJakartaServlet servlet = new RAPJakartaServlet(mockTransportDispatcher, TIMEOUT_SECONDS);
    assertNotNull(servlet.getDispatcher());
  }

  @Test
  public void testSuccessfulRequest() throws ServletException, IOException {
    // Setup
    RAPJakartaServlet servlet = new RAPJakartaServlet(mockHttpDispatcher, TIMEOUT_SECONDS);
    RestResponse mockRestResponse = RestStatus.responseForStatus(200, "OK");
    TransportResponse<RestResponse> transportResponse =
        TransportResponseImpl.success(mockRestResponse);

    doAnswer(
            invocation -> {
              TransportCallback<RestResponse> callback = invocation.getArgument(2);
              callback.onResponse(transportResponse);
              return null;
            })
        .when(mockHttpDispatcher)
        .handleRequest(
            any(RestRequest.class), any(RequestContext.class), any(TransportCallback.class));

    // Execute
    servlet.service(mockRequest, mockResponse);

    // Verify status code
    verify(mockResponse).setStatus(200);

    // Capture and verify the output stream write
    ArgumentCaptor<byte[]> dataCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);

    verify(mockOutputStream)
        .write(dataCaptor.capture(), offsetCaptor.capture(), lengthCaptor.capture());

    // Convert captured bytes to string for readable verification
    byte[] capturedData = dataCaptor.getValue();
    int offset = offsetCaptor.getValue();
    int length = lengthCaptor.getValue();

    String responseBody = new String(capturedData, offset, length);
    System.out.println("Actual success response body: " + responseBody); // For debugging

    assertEquals(responseBody, "OK", "Expected response body to be 'OK' but got: " + responseBody);
  }

  @Test
  public void testSecureRequest() throws ServletException, IOException {
    // Setup secure request
    X509Certificate[] certs = new X509Certificate[1];
    certs[0] = mock(X509Certificate.class);
    String cipherSuite = "TLS_AES_256_GCM_SHA384";

    when(mockRequest.isSecure()).thenReturn(true);
    when(mockRequest.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(certs);
    when(mockRequest.getAttribute("javax.servlet.request.cipher_suite")).thenReturn(cipherSuite);

    RAPJakartaServlet servlet = new RAPJakartaServlet(mockHttpDispatcher, TIMEOUT_SECONDS);

    ArgumentCaptor<RequestContext> contextCaptor = ArgumentCaptor.forClass(RequestContext.class);

    // Execute
    servlet.service(mockRequest, mockResponse);

    // Verify
    verify(mockHttpDispatcher)
        .handleRequest(
            any(RestRequest.class), contextCaptor.capture(), any(TransportCallback.class));

    RequestContext capturedContext = contextCaptor.getValue();
    assertTrue((Boolean) capturedContext.getLocalAttr(R2Constants.IS_SECURE));
    assertEquals(capturedContext.getLocalAttr(R2Constants.CIPHER_SUITE), cipherSuite);
    assertEquals(capturedContext.getLocalAttr(R2Constants.CLIENT_CERT), certs[0]);
  }

  @Test
  public void testBadRequest() throws ServletException, IOException {
    // Setup with invalid URI components that will cause URISyntaxException
    when(mockRequest.getRequestURI()).thenReturn("/test");
    when(mockRequest.getQueryString()).thenReturn("invalid=%%test"); // Invalid percent encoding
    when(mockRequest.getContextPath()).thenReturn("");
    when(mockRequest.getServletPath()).thenReturn("");

    RAPJakartaServlet servlet = new RAPJakartaServlet(mockHttpDispatcher, TIMEOUT_SECONDS);

    // Execute
    servlet.service(mockRequest, mockResponse);

    // Verify
    verify(mockResponse).setStatus(400);

    // Capture all write invocations to the output stream
    ArgumentCaptor<byte[]> dataCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);

    verify(mockOutputStream)
        .write(dataCaptor.capture(), offsetCaptor.capture(), lengthCaptor.capture());

    // Convert captured bytes to string for readable verification
    byte[] capturedData = dataCaptor.getValue();
    int offset = offsetCaptor.getValue();
    int length = lengthCaptor.getValue();

    String responseBody = new String(capturedData, offset, length);
    System.out.println("Actual response body: " + responseBody); // For debugging

    assertTrue(
        responseBody.contains("Invalid URI"),
        "Expected response to contain 'Invalid URI' but got: " + responseBody);
  }

  @Test
  public void testRequestTimeout() throws ServletException, IOException {
    // Setup with very short timeout
    RAPJakartaServlet servlet = new RAPJakartaServlet(mockHttpDispatcher, 1);

    doAnswer(
            invocation -> {
              Thread.sleep(2000);
              return null;
            })
        .when(mockHttpDispatcher)
        .handleRequest(
            any(RestRequest.class), any(RequestContext.class), any(TransportCallback.class));

    // Execute
    servlet.service(mockRequest, mockResponse);

    // Verify status code
    verify(mockResponse).setStatus(500);

    // Capture and verify the output stream write
    ArgumentCaptor<byte[]> dataCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);

    verify(mockOutputStream)
        .write(dataCaptor.capture(), offsetCaptor.capture(), lengthCaptor.capture());

    // Convert captured bytes to string for readable verification
    byte[] capturedData = dataCaptor.getValue();
    int offset = offsetCaptor.getValue();
    int length = lengthCaptor.getValue();

    String responseBody = new String(capturedData, offset, length);
    System.out.println("Actual timeout response body: " + responseBody); // For debugging

    assertTrue(
        responseBody.contains("Server timeout after 1 seconds"),
        "Expected response to contain timeout message but got: " + responseBody);
  }

  @DataProvider(name = "headerTestCases")
  public Object[][] headerTestCases() {
    return new Object[][] {
      {"Content-Type", "application/json"},
      {"Accept", "text/plain"},
      {"X-Custom-Header", "custom-value"}
    };
  }

  @Test(dataProvider = "headerTestCases")
  public void testHeaderHandling(String headerName, String headerValue)
      throws ServletException, IOException {
    // Setup
    RAPJakartaServlet servlet = new RAPJakartaServlet(mockHttpDispatcher, TIMEOUT_SECONDS);
    when(mockRequest.getHeaderNames())
        .thenReturn(Collections.enumeration(Collections.singletonList(headerName)));
    when(mockRequest.getHeaders(headerName))
        .thenReturn(Collections.enumeration(Collections.singletonList(headerValue)));

    ArgumentCaptor<RestRequest> requestCaptor = ArgumentCaptor.forClass(RestRequest.class);

    // Execute
    servlet.service(mockRequest, mockResponse);

    // Verify
    verify(mockHttpDispatcher)
        .handleRequest(
            requestCaptor.capture(), any(RequestContext.class), any(TransportCallback.class));
    RestRequest capturedRequest = requestCaptor.getValue();
    assertEquals(capturedRequest.getHeader(headerName), headerValue);
  }

  private static class TestServletInputStream extends jakarta.servlet.ServletInputStream {
    private final ByteArrayInputStream basis;

    public TestServletInputStream(ByteArrayInputStream basis) {
      this.basis = basis;
    }

    @Override
    public int read() throws IOException {
      return basis.read();
    }

    @Override
    public boolean isFinished() {
      return basis.available() == 0;
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setReadListener(jakarta.servlet.ReadListener readListener) {
      // Not implemented for test
    }
  }
}
