package io.datahubproject.metadata.context.usage.instrumentation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CountingHttpServletResponseWrapperTest {

  private ByteArrayOutputStream body;
  private HttpServletResponse delegate;
  private CountingHttpServletResponseWrapper wrapper;

  @BeforeMethod
  public void setUp() throws IOException {
    body = new ByteArrayOutputStream();
    delegate = Mockito.mock(HttpServletResponse.class);
    Mockito.when(delegate.getOutputStream()).thenReturn(new TestServletOutputStream(body));
    Mockito.when(delegate.getCharacterEncoding()).thenReturn(StandardCharsets.UTF_8.name());
    wrapper = new CountingHttpServletResponseWrapper(delegate);
  }

  @Test
  public void testCountsOutputStreamWrites() throws IOException {
    wrapper.getOutputStream().write("hello".getBytes(StandardCharsets.UTF_8));

    assertEquals(wrapper.getBytesWritten(), 5L);
    assertEquals(body.toString(StandardCharsets.UTF_8), "hello");
  }

  @Test
  public void testCountsWriterWrites() throws IOException {
    PrintWriter writer = wrapper.getWriter();
    writer.write("hello");

    assertEquals(wrapper.getBytesWritten(), 5L);
    assertEquals(body.toString(StandardCharsets.UTF_8), "hello");
  }

  @Test
  public void testRejectsMixedOutputStreamAndWriter() throws IOException {
    wrapper.getOutputStream();

    assertThrows(IllegalStateException.class, wrapper::getWriter);
  }

  @Test
  public void testRejectsMixedWriterThenOutputStream() throws IOException {
    wrapper.getWriter();

    assertThrows(IllegalStateException.class, wrapper::getOutputStream);
  }

  @Test
  public void testCountsSingleByteWrites() throws IOException {
    ServletOutputStream stream = wrapper.getOutputStream();
    stream.write('a');
    stream.write('b');
    stream.flush();
    stream.close();

    assertEquals(wrapper.getBytesWritten(), 2L);
  }

  @Test
  public void testGetOutputStreamIsReusable() throws IOException {
    ServletOutputStream first = wrapper.getOutputStream();
    ServletOutputStream second = wrapper.getOutputStream();
    assertEquals(first, second);
  }

  @Test
  public void testGetWriterIsReusable() throws IOException {
    PrintWriter first = wrapper.getWriter();
    PrintWriter second = wrapper.getWriter();
    assertEquals(first, second);
  }

  @Test
  public void testWriterUsesDefaultCharsetWhenEncodingMissing() throws IOException {
    Mockito.when(delegate.getCharacterEncoding()).thenReturn(null);
    wrapper = new CountingHttpServletResponseWrapper(delegate);

    PrintWriter writer = wrapper.getWriter();
    writer.write("x");

    assertEquals(wrapper.getBytesWritten(), 1L);
  }

  @Test
  public void testOutputStreamReadyAndWriteListenerDelegate() throws IOException {
    ServletOutputStream stream = wrapper.getOutputStream();
    assertEquals(stream.isReady(), true);
    stream.setWriteListener(null);
  }

  private static final class TestServletOutputStream extends ServletOutputStream {

    private final ByteArrayOutputStream delegate;

    private TestServletOutputStream(ByteArrayOutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(int b) {
      delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      delegate.write(b, off, len);
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setWriteListener(WriteListener writeListener) {}
  }
}
