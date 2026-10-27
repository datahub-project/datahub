package io.datahubproject.metadata.context.usage.instrumentation;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import javax.annotation.Nonnull;

/** Counts response body bytes written to the underlying servlet output stream. */
public class CountingHttpServletResponseWrapper extends HttpServletResponseWrapper {

  private long bytesWritten;
  private ServletOutputStream outputStream;
  private PrintWriter writer;

  public CountingHttpServletResponseWrapper(@Nonnull HttpServletResponse response) {
    super(response);
  }

  public long getBytesWritten() {
    if (writer != null) {
      writer.flush();
    }
    return bytesWritten;
  }

  @Override
  public ServletOutputStream getOutputStream() throws IOException {
    if (writer != null) {
      throw new IllegalStateException("getWriter() has already been called on this response");
    }
    if (outputStream == null) {
      outputStream =
          new CountingServletOutputStream(getResponse().getOutputStream(), this::addBytes);
    }
    return outputStream;
  }

  @Override
  public PrintWriter getWriter() throws IOException {
    if (outputStream != null) {
      throw new IllegalStateException("getOutputStream() has already been called on this response");
    }
    if (writer == null) {
      String encoding = getCharacterEncoding();
      Charset charset = encoding != null ? Charset.forName(encoding) : Charset.defaultCharset();
      writer =
          new PrintWriter(
              new OutputStreamWriter(
                  new CountingServletOutputStream(getResponse().getOutputStream(), this::addBytes),
                  charset));
    }
    return writer;
  }

  private void addBytes(long count) {
    bytesWritten += count;
  }

  private static final class CountingServletOutputStream extends ServletOutputStream {

    private final ServletOutputStream delegate;
    private final ByteCounter counter;

    @FunctionalInterface
    private interface ByteCounter {
      void add(long count);
    }

    private CountingServletOutputStream(
        @Nonnull ServletOutputStream delegate, @Nonnull ByteCounter counter) {
      this.delegate = delegate;
      this.counter = counter;
    }

    @Override
    public void write(int b) throws IOException {
      delegate.write(b);
      counter.add(1);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      delegate.write(b, off, len);
      counter.add(len);
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public boolean isReady() {
      return delegate.isReady();
    }

    @Override
    public void setWriteListener(WriteListener writeListener) {
      delegate.setWriteListener(writeListener);
    }
  }
}
