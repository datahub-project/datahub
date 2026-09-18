package com.datahub.graphql;

import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.server.ServletServerHttpResponse;
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

  private static ServletResponseFixture servletResponse(boolean committed) throws IOException {
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.isCommitted()).thenReturn(committed);
    when(response.getOutputStream())
        .thenReturn(
            new ServletOutputStream() {
              @Override
              public boolean isReady() {
                return true;
              }

              @Override
              public void setWriteListener(WriteListener writeListener) {}

              @Override
              public void write(int b) {
                sink.write(b);
              }

              @Override
              public void write(byte[] b, int off, int len) {
                sink.write(b, off, len);
              }
            });
    org.mockito.Mockito.doAnswer(
            invocation -> {
              sink.reset();
              return null;
            })
        .when(response)
        .resetBuffer();
    return new ServletResponseFixture(response, sink);
  }

  private record ServletResponseFixture(HttpServletResponse response, ByteArrayOutputStream sink) {}

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
    boolean[] finished = {false};

    converter.write(
        new GraphQLResponseBody(spec, b -> recorded[0] = b, success -> finished[0] = success),
        MediaType.APPLICATION_JSON,
        outputMessage(sink, headers));

    byte[] streamed = sink.toByteArray();
    assertEquals(recorded[0], streamed.length, "callback must report the bytes actually written");
    assertTrue(finished[0], "successful write must report successful completion");
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
        new GraphQLResponseBody(spec, b -> recorded[0] = b, success -> {}),
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
    boolean[] finished = {true};

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
          new GraphQLResponseBody(
              Map.of("data", Map.of("k", "v")),
              b -> callbackInvoked[0] = true,
              success -> finished[0] = success),
          MediaType.APPLICATION_JSON,
          outputMessage(failing, new HttpHeaders()));
    } finally {
      // Genuine failure: no byte callback, counted as streamError (not clientAbort).
      assertFalse(callbackInvoked[0]);
      assertFalse(finished[0], "failed write must report unsuccessful completion");
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
    boolean[] finished = {true};

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
          new GraphQLResponseBody(
              Map.of("data", Map.of("k", "v")),
              b -> callbackInvoked[0] = true,
              success -> finished[0] = success),
          MediaType.APPLICATION_JSON,
          outputMessage(aborting, new HttpHeaders()));
    } finally {
      // Client disconnect: counted as clientAbort, never streamError, no byte callback.
      assertFalse(callbackInvoked[0]);
      assertFalse(finished[0], "client abort must report unsuccessful completion");
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
    boolean[] finished = {true};

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
          new GraphQLResponseBody(
              Map.of("data", Map.of("k", "v")),
              b -> callbackInvoked[0] = true,
              success -> finished[0] = success),
          MediaType.APPLICATION_JSON,
          outputMessage(aborting, new HttpHeaders()));
    } finally {
      assertFalse(callbackInvoked[0]);
      assertFalse(finished[0], "wrapped client abort must report unsuccessful completion");
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

  @Test(expectedExceptions = GraphQLResponseSerializationException.class)
  public void testUncommittedSerializationFailureResetsBufferAndUsesSerializeError()
      throws Exception {
    ObjectMapper mapper = mock(ObjectMapper.class);
    MetricUtils metricUtils = mock(MetricUtils.class);
    GraphQLResponseBodyConverter converter = new GraphQLResponseBodyConverter(mapper, metricUtils);
    ServletResponseFixture fixture = servletResponse(false);
    boolean[] finished = {true};

    org.mockito.Mockito.doAnswer(
            invocation -> {
              Writer writer = invocation.getArgument(0);
              writer.write("{\"partial\":");
              writer.flush();
              throw new JsonProcessingException("cannot serialize") {};
            })
        .when(mapper)
        .writeValue(
            org.mockito.ArgumentMatchers.any(Writer.class), org.mockito.ArgumentMatchers.any());

    try {
      converter.writeInternal(
          new GraphQLResponseBody(
              Map.of("data", "bad"), bytes -> {}, success -> finished[0] = success),
          new ServletServerHttpResponse(fixture.response()));
    } finally {
      assertFalse(fixture.response().isCommitted());
      assertEquals(fixture.sink().size(), 0, "partial JSON must be reset");
      assertFalse(finished[0]);
      verify(fixture.response(), times(1)).resetBuffer();
      verify(metricUtils, times(1))
          .increment(
              GraphQLResponseBodyConverter.class,
              GraphQLResponseBodyConverter.SERIALIZE_ERROR_METRIC,
              1);
      verify(metricUtils, never())
          .increment(
              eq(GraphQLResponseBodyConverter.class),
              eq(GraphQLResponseBodyConverter.STREAM_ERROR_METRIC),
              anyDouble());
    }
  }

  @Test(expectedExceptions = EOFException.class)
  public void testUncommittedClientAbortStaysClientAbortNotSerializeError() throws Exception {
    ObjectMapper mapper = mock(ObjectMapper.class);
    MetricUtils metricUtils = mock(MetricUtils.class);
    GraphQLResponseBodyConverter converter = new GraphQLResponseBodyConverter(mapper, metricUtils);
    ServletResponseFixture fixture = servletResponse(false);

    org.mockito.Mockito.doThrow(new EOFException("early EOF"))
        .when(mapper)
        .writeValue(
            org.mockito.ArgumentMatchers.any(Writer.class), org.mockito.ArgumentMatchers.any());

    try {
      converter.writeInternal(
          new GraphQLResponseBody(Map.of("data", "v"), bytes -> {}, success -> {}),
          new ServletServerHttpResponse(fixture.response()));
    } finally {
      verify(metricUtils, times(1))
          .increment(
              GraphQLResponseBodyConverter.class,
              GraphQLResponseBodyConverter.CLIENT_ABORT_METRIC,
              1);
      verify(metricUtils, never())
          .increment(
              eq(GraphQLResponseBodyConverter.class),
              eq(GraphQLResponseBodyConverter.SERIALIZE_ERROR_METRIC),
              anyDouble());
    }
  }

  @Test(expectedExceptions = JsonProcessingException.class)
  public void testCommittedSerializationFailureStaysStreamErrorAndDoesNotResetBuffer()
      throws Exception {
    ObjectMapper mapper = mock(ObjectMapper.class);
    MetricUtils metricUtils = mock(MetricUtils.class);
    GraphQLResponseBodyConverter converter = new GraphQLResponseBodyConverter(mapper, metricUtils);
    ServletResponseFixture fixture = servletResponse(true);

    org.mockito.Mockito.doThrow(new JsonProcessingException("cannot serialize") {})
        .when(mapper)
        .writeValue(
            org.mockito.ArgumentMatchers.any(Writer.class), org.mockito.ArgumentMatchers.any());

    try {
      converter.writeInternal(
          new GraphQLResponseBody(Map.of("data", "bad"), bytes -> {}, success -> {}),
          new ServletServerHttpResponse(fixture.response()));
    } finally {
      verify(fixture.response(), never()).resetBuffer();
      verify(metricUtils, times(1))
          .increment(
              GraphQLResponseBodyConverter.class,
              GraphQLResponseBodyConverter.STREAM_ERROR_METRIC,
              1);
      verify(metricUtils, never())
          .increment(
              eq(GraphQLResponseBodyConverter.class),
              eq(GraphQLResponseBodyConverter.SERIALIZE_ERROR_METRIC),
              anyDouble());
    }
  }

  @Test
  public void testCompletionCallbackCannotMaskPreCommitSerializationException() throws Exception {
    ObjectMapper mapper = mock(ObjectMapper.class);
    GraphQLResponseBodyConverter converter = new GraphQLResponseBodyConverter(mapper, null);
    ServletResponseFixture fixture = servletResponse(false);
    org.mockito.Mockito.doThrow(new JsonProcessingException("cannot serialize") {})
        .when(mapper)
        .writeValue(
            org.mockito.ArgumentMatchers.any(Writer.class), org.mockito.ArgumentMatchers.any());

    assertThrows(
        GraphQLResponseSerializationException.class,
        () ->
            converter.writeInternal(
                new GraphQLResponseBody(
                    Map.of("data", "bad"),
                    bytes -> {},
                    success -> {
                      throw new IllegalStateException("release failed");
                    }),
                new ServletServerHttpResponse(fixture.response())));
  }

  @Test
  public void testResetBufferCommitRaceFallsBackToStreamError() throws Exception {
    ObjectMapper mapper = mock(ObjectMapper.class);
    MetricUtils metricUtils = mock(MetricUtils.class);
    GraphQLResponseBodyConverter converter = new GraphQLResponseBodyConverter(mapper, metricUtils);
    ServletResponseFixture fixture = servletResponse(false);
    org.mockito.Mockito.doThrow(new IllegalStateException("already committed"))
        .when(fixture.response())
        .resetBuffer();
    JsonProcessingException failure = new JsonProcessingException("cannot serialize") {};
    org.mockito.Mockito.doThrow(failure)
        .when(mapper)
        .writeValue(
            org.mockito.ArgumentMatchers.any(Writer.class), org.mockito.ArgumentMatchers.any());

    JsonProcessingException actual =
        expectThrows(
            JsonProcessingException.class,
            () ->
                converter.writeInternal(
                    new GraphQLResponseBody(Map.of("data", "bad"), bytes -> {}, success -> {}),
                    new ServletServerHttpResponse(fixture.response())));

    assertEquals(actual, failure);
    verify(metricUtils, times(1))
        .increment(
            GraphQLResponseBodyConverter.class,
            GraphQLResponseBodyConverter.STREAM_ERROR_METRIC,
            1);
    verify(metricUtils, never())
        .increment(
            eq(GraphQLResponseBodyConverter.class),
            eq(GraphQLResponseBodyConverter.SERIALIZE_ERROR_METRIC),
            anyDouble());
  }

  @Test
  public void testMetricFailureCannotMaskPreCommitSerializationException() throws Exception {
    ObjectMapper mapper = mock(ObjectMapper.class);
    MetricUtils metricUtils = mock(MetricUtils.class);
    GraphQLResponseBodyConverter converter = new GraphQLResponseBodyConverter(mapper, metricUtils);
    ServletResponseFixture fixture = servletResponse(false);
    org.mockito.Mockito.doThrow(new JsonProcessingException("cannot serialize") {})
        .when(mapper)
        .writeValue(
            org.mockito.ArgumentMatchers.any(Writer.class), org.mockito.ArgumentMatchers.any());
    org.mockito.Mockito.doThrow(new IllegalStateException("metrics unavailable"))
        .when(metricUtils)
        .increment(
            GraphQLResponseBodyConverter.class,
            GraphQLResponseBodyConverter.SERIALIZE_ERROR_METRIC,
            1);

    assertThrows(
        GraphQLResponseSerializationException.class,
        () ->
            converter.writeInternal(
                new GraphQLResponseBody(Map.of("data", "bad"), bytes -> {}, success -> {}),
                new ServletServerHttpResponse(fixture.response())));
  }
}
