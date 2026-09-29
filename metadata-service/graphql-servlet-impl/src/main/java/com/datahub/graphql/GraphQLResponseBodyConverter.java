package com.datahub.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import jakarta.servlet.http.HttpServletResponse;
import java.io.EOFException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.server.ServletServerHttpResponse;

/**
 * Streams a {@link GraphQLResponseBody} straight to the response socket, so the full response is
 * never materialized as one String/char[] (the dominant heap driver behind GMS OOMs on large
 * fan-out queries).
 *
 * <p>A plain {@code ResponseEntity} body (rather than a {@code StreamingResponseBody}) is used
 * deliberately: it is written here during async dispatch on the servlet (dispatch) thread — not the
 * MVC async task executor, which is an unbounded {@code SimpleAsyncTaskExecutor} under
 * {@code @EnableWebMvc}, and not under the async request timeout. Write-only.
 */
@Slf4j
public class GraphQLResponseBodyConverter
    extends AbstractHttpMessageConverter<GraphQLResponseBody> {

  /** Metric: a genuine server-side mid-stream serialization failure. */
  static final String STREAM_ERROR_METRIC = "streamError";

  /**
   * Metric: serialization failed before commit and the response can be replaced with a clean 503.
   */
  static final String SERIALIZE_ERROR_METRIC = "serializeError";

  /** Metric: client disconnected mid-stream (routine, not a server error). */
  static final String CLIENT_ABORT_METRIC = "clientAbort";

  private final ObjectMapper mapper;
  @Nullable private final MetricUtils metricUtils;

  public GraphQLResponseBodyConverter(
      @Nonnull final ObjectMapper mapper, @Nullable final MetricUtils metricUtils) {
    super(MediaType.APPLICATION_JSON);
    this.mapper = mapper;
    this.metricUtils = metricUtils;
  }

  @Override
  protected boolean supports(Class<?> clazz) {
    return GraphQLResponseBody.class.isAssignableFrom(clazz);
  }

  @Override
  public boolean canRead(Class<?> clazz, MediaType mediaType) {
    return false;
  }

  @Override
  protected GraphQLResponseBody readInternal(
      Class<? extends GraphQLResponseBody> clazz, HttpInputMessage inputMessage) {
    throw new HttpMessageNotReadableException("GraphQLResponseBody is write-only", inputMessage);
  }

  @Override
  protected void writeInternal(GraphQLResponseBody body, HttpOutputMessage outputMessage)
      throws IOException {
    final CountingOutputStream counting = new CountingOutputStream(outputMessage.getBody());
    boolean writeSucceeded = false;
    Throwable primaryFailure = null;
    try {
      try {
        // UTF-8 Writer (not writeValue(OutputStream)): same char generator as the buffered
        // writeValueAsString path, so wire bytes are identical across the flag — incl. raw UTF-8
        // for supplementary chars (emoji) that the byte generator would \\u-escape instead.
        mapper.writeValue(new OutputStreamWriter(counting, StandardCharsets.UTF_8), body.spec());
        writeSucceeded = true;
      } catch (IOException | RuntimeException e) {
        final boolean clientAbort = isClientAbort(e);
        final HttpServletResponse servletResponse =
            outputMessage instanceof ServletServerHttpResponse servletOutput
                ? servletOutput.getServletResponse()
                : null;

        if (clientAbort) {
          incrementMetric(CLIENT_ABORT_METRIC);
          log.debug(
              "GraphQL response streaming aborted by client after {} bytes: {}",
              counting.getCount(),
              e.toString());
          GraphQLResponseStreamAbortedException aborted =
              new GraphQLResponseStreamAbortedException(e);
          primaryFailure = aborted;
          throw aborted;
        }

        if (servletResponse != null && !servletResponse.isCommitted()) {
          try {
            // Jackson and the servlet both buffer, so byte count alone cannot tell whether headers
            // are committed. Clear partial servlet-buffered JSON before the 503 handler writes.
            servletResponse.resetBuffer();
            incrementMetric(SERIALIZE_ERROR_METRIC);
            log.error("Failed to serialize GraphQL response before commit", e);
            GraphQLResponseSerializationException serializationException =
                new GraphQLResponseSerializationException(e);
            primaryFailure = serializationException;
            throw serializationException;
          } catch (IllegalStateException commitRace) {
            // The container committed between isCommitted() and resetBuffer(); fall through to the
            // irrecoverable mid-stream path and preserve the original serialization failure.
            log.debug("GraphQL response committed before its buffer could be reset", commitRace);
          }
        }

        incrementMetric(STREAM_ERROR_METRIC);
        // Once committed, HTTP 200 and any bytes already sent cannot be replaced.
        log.error("Failed to stream GraphQL response after {} bytes", counting.getCount(), e);
        GraphQLResponseStreamAbortedException aborted =
            new GraphQLResponseStreamAbortedException(e);
        primaryFailure = aborted;
        throw aborted;
      }
      body.onBytesWritten().accept(counting.getCount());
    } finally {
      try {
        body.onWriteFinished().accept(writeSucceeded);
      } catch (RuntimeException completionFailure) {
        if (primaryFailure != null) {
          primaryFailure.addSuppressed(completionFailure);
          log.error("Failed to complete GraphQL response write lifecycle", completionFailure);
        } else {
          throw completionFailure;
        }
      }
    }
  }

  private void incrementMetric(String metric) {
    if (metricUtils != null) {
      try {
        metricUtils.increment(getClass(), metric, 1);
      } catch (RuntimeException metricFailure) {
        // Observability must never change response status or mask the primary socket/serialization
        // failure.
        log.warn("Failed to increment GraphQL response metric {}", metric, metricFailure);
      }
    }
  }

  /**
   * True when a mid-stream {@link IOException} is a client disconnect: it (or a cause) is an {@link
   * EOFException} (Jetty's {@code EofException} extends it — no jetty dependency needed), is a
   * container abort ({@code EofException}/{@code ClientAbortException} by name), or names a
   * broken/reset connection. Not a bare "closed", so a real serialization failure still counts.
   */
  private static boolean isClientAbort(@Nullable Throwable t) {
    for (int depth = 0; t != null && depth < 16; t = t.getCause(), depth++) {
      final String simpleName = t.getClass().getSimpleName();
      if (t instanceof EOFException
          || simpleName.equals("EofException")
          || simpleName.equals("ClientAbortException")) {
        return true;
      }
      final String message = t.getMessage();
      if (message != null) {
        final String m = message.toLowerCase(Locale.ROOT);
        if (m.contains("broken pipe")
            || m.contains("connection reset")
            || m.contains("reset by peer")) {
          return true;
        }
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return false;
  }

  /**
   * Counts bytes written so the response-size metric can report the streamed size without buffering
   * the whole response. {@code close()} flushes but does not close the underlying servlet stream —
   * Spring owns that lifecycle.
   */
  private static final class CountingOutputStream extends FilterOutputStream {
    private long count;

    CountingOutputStream(OutputStream out) {
      super(out);
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
      count++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
      count += len;
    }

    @Override
    public void close() throws IOException {
      out.flush();
    }

    long getCount() {
      return count;
    }
  }
}
