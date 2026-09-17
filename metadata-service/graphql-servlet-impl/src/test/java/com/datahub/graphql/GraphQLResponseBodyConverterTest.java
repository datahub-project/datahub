package com.datahub.graphql;

import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.testng.annotations.Test;

public class GraphQLResponseBodyConverterTest {

  private static HttpOutputMessage outputMessage(OutputStream body, HttpHeaders headers) {
    return new HttpOutputMessage() {
      @Override
      public OutputStream getBody() {
        return body;
      }

      @Override
      public HttpHeaders getHeaders() {
        return headers;
      }
    };
  }

  @Test
  public void testWriteStreamsCompleteJsonAndReportsStreamedByteCount() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    GraphQLResponseBodyConverter converter = new GraphQLResponseBodyConverter(mapper, null);

    // Explicit null value included to confirm the streamed shape keeps nulls (not NON_NULL).
    Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("urn", "urn:li:corpuser:datahub");
    inner.put("description", null);
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("data", Map.of("me", inner));

    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    HttpHeaders headers = new HttpHeaders();
    long[] recorded = {-1L};

    converter.write(
        new GraphQLResponseBody(spec, b -> recorded[0] = b),
        MediaType.APPLICATION_JSON,
        outputMessage(sink, headers));

    byte[] streamed = sink.toByteArray();
    assertEquals(recorded[0], streamed.length, "callback must report the bytes actually written");
    assertTrue(headers.getContentType().includes(MediaType.APPLICATION_JSON));

    JsonNode parsed = mapper.readTree(streamed);
    assertEquals(parsed.path("data").path("me").path("urn").asText(), "urn:li:corpuser:datahub");
    assertTrue(
        parsed.path("data").path("me").has("description"), "explicit null must be preserved");
    assertTrue(parsed.path("data").path("me").path("description").isNull());
  }

  @Test
  public void testStreamedBytesAreIdenticalToBufferedIncludingSupplementaryChars()
      throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    GraphQLResponseBodyConverter converter = new GraphQLResponseBodyConverter(mapper, null);

    // Includes a supplementary char (😀): the byte generator would \\u-escape it; the Writer path
    // must emit raw UTF-8, matching writeValueAsString byte-for-byte.
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("data", Map.of("v", "é汉😀"));

    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    long[] recorded = {-1L};
    converter.write(
        new GraphQLResponseBody(spec, b -> recorded[0] = b),
        MediaType.APPLICATION_JSON,
        outputMessage(sink, new HttpHeaders()));

    byte[] streamed = sink.toByteArray();
    byte[] buffered = mapper.writeValueAsString(spec).getBytes(StandardCharsets.UTF_8);
    assertEquals(streamed, buffered, "streamed bytes must equal the buffered path byte-for-byte");
    assertEquals(recorded[0], streamed.length, "recorded size must be the true UTF-8 byte count");
  }

  @Test(expectedExceptions = IOException.class)
  public void testWritePropagatesMidStreamIoErrorCountsItAndSkipsCallback() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    MetricUtils metricUtils = mock(MetricUtils.class);
    GraphQLResponseBodyConverter converter = new GraphQLResponseBodyConverter(mapper, metricUtils);
    boolean[] callbackInvoked = {false};

    OutputStream failing =
        new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            throw new IOException("socket closed");
          }

          @Override
          public void write(byte[] b, int off, int len) throws IOException {
            throw new IOException("socket closed");
          }
        };

    try {
      converter.write(
          new GraphQLResponseBody(Map.of("data", Map.of("k", "v")), b -> callbackInvoked[0] = true),
          MediaType.APPLICATION_JSON,
          outputMessage(failing, new HttpHeaders()));
    } finally {
      // Genuine failure: no byte callback, counted as streamError (not clientAbort).
      assertFalse(callbackInvoked[0]);
      verify(metricUtils, times(1))
          .increment(
              GraphQLResponseBodyConverter.class,
              GraphQLResponseBodyConverter.STREAM_ERROR_METRIC,
              1);
      verify(metricUtils, never())
          .increment(
              eq(GraphQLResponseBodyConverter.class),
              eq(GraphQLResponseBodyConverter.CLIENT_ABORT_METRIC),
              anyDouble());
    }
  }

  @Test(expectedExceptions = EOFException.class)
  public void testWriteClientAbortCountsClientAbortNotStreamError() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    MetricUtils metricUtils = mock(MetricUtils.class);
    GraphQLResponseBodyConverter converter = new GraphQLResponseBodyConverter(mapper, metricUtils);
    boolean[] callbackInvoked = {false};

    // Jetty surfaces a client disconnect as EofException (extends EOFException); simulate it.
    OutputStream aborting =
        new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            throw new EOFException("early EOF");
          }

          @Override
          public void write(byte[] b, int off, int len) throws IOException {
            throw new EOFException("early EOF");
          }
        };

    try {
      converter.write(
          new GraphQLResponseBody(Map.of("data", Map.of("k", "v")), b -> callbackInvoked[0] = true),
          MediaType.APPLICATION_JSON,
          outputMessage(aborting, new HttpHeaders()));
    } finally {
      // Client disconnect: counted as clientAbort, never streamError, no byte callback.
      assertFalse(callbackInvoked[0]);
      verify(metricUtils, times(1))
          .increment(
              GraphQLResponseBodyConverter.class,
              GraphQLResponseBodyConverter.CLIENT_ABORT_METRIC,
              1);
      verify(metricUtils, never())
          .increment(
              eq(GraphQLResponseBodyConverter.class),
              eq(GraphQLResponseBodyConverter.STREAM_ERROR_METRIC),
              anyDouble());
    }
  }

  @Test
  public void testConverterIsWriteOnly() {
    GraphQLResponseBodyConverter converter =
        new GraphQLResponseBodyConverter(new ObjectMapper(), null);
    // Write-only: it must never be selected to deserialize a request body.
    assertFalse(converter.canRead(GraphQLResponseBody.class, MediaType.APPLICATION_JSON));
  }

  @Test(expectedExceptions = IOException.class)
  public void testWriteClientAbortDetectedViaCauseChain() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    MetricUtils metricUtils = mock(MetricUtils.class);
    GraphQLResponseBodyConverter converter = new GraphQLResponseBodyConverter(mapper, metricUtils);
    boolean[] callbackInvoked = {false};

    // Abort wrapped inside a generic IOException — exercises the cause-chain walk (the top-level
    // exception is not itself an EOFException, so a depth-0-only check would misclassify it).
    OutputStream aborting =
        new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            throw new IOException("stream broke", new EOFException("early EOF"));
          }

          @Override
          public void write(byte[] b, int off, int len) throws IOException {
            throw new IOException("stream broke", new EOFException("early EOF"));
          }
        };

    try {
      converter.write(
          new GraphQLResponseBody(Map.of("data", Map.of("k", "v")), b -> callbackInvoked[0] = true),
          MediaType.APPLICATION_JSON,
          outputMessage(aborting, new HttpHeaders()));
    } finally {
      assertFalse(callbackInvoked[0]);
      verify(metricUtils, times(1))
          .increment(
              GraphQLResponseBodyConverter.class,
              GraphQLResponseBodyConverter.CLIENT_ABORT_METRIC,
              1);
      verify(metricUtils, never())
          .increment(
              eq(GraphQLResponseBodyConverter.class),
              eq(GraphQLResponseBodyConverter.STREAM_ERROR_METRIC),
              anyDouble());
    }
  }
}
