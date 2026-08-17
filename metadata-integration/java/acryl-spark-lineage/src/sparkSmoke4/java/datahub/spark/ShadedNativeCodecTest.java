package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Regression guard for the JNI-backed compression codecs reachable from the shaded agent — the same
 * failure mode as issue #18558 (relocated Java class vs. native symbols compiled under the
 * canonical package name).
 *
 * <p>The agent bundles a Kafka client for its Kafka emitter, and {@code
 * spark.datahub.kafka.producer_config.compression.type} lets users select snappy/zstd/lz4. Those
 * codecs are JNI-backed: their native libraries export {@code Java_org_xerial_snappy_*} and
 * friends, so relocating the Java classes under {@code io.acryl.shaded} left the JVM hunting for
 * symbols that do not exist in the binary — {@code UnsatisfiedLinkError} on the first compressed
 * produce.
 *
 * <p>The codecs must therefore resolve at their canonical names, where Spark supplies them (Spark 4
 * ships snappy-java, zstd-jni and an lz4-java fork). Removing the bundled copies without also
 * dropping the relocation would merely convert the error into {@code NoClassDefFoundError}, since
 * the shaded Kafka client's bytecode still references the relocated names. This test drives Kafka's
 * real compression path, so it fails in either case.
 */
public class ShadedNativeCodecTest {

  private static final String COMPRESSION =
      "io.acryl.shaded.org.apache.kafka.common.compress.Compression";
  private static final String BB_OUTPUT_STREAM =
      "io.acryl.shaded.org.apache.kafka.common.utils.ByteBufferOutputStream";
  private static final String BUFFER_SUPPLIER =
      "io.acryl.shaded.org.apache.kafka.common.utils.BufferSupplier";
  private static final byte MAGIC_V2 = 2;

  @Test
  public void shadedKafkaClientCanCompressWithEveryCodec() throws Exception {
    // gzip is pure Java and acts as a control: if it fails too, the problem is the test, not the
    // JNI.
    for (String codec : new String[] {"gzip", "snappy", "lz4", "zstd"}) {
      assertCompresses(codec);
    }
  }

  private static void assertCompresses(String codec) throws Exception {
    byte[] payload = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);

    Class<?> compressionClass = Class.forName(COMPRESSION);
    Class<?> sinkClass = Class.forName(BB_OUTPUT_STREAM);
    Object builder = compressionClass.getMethod("of", String.class).invoke(null, codec);
    Object compression = builder.getClass().getMethod("build").invoke(builder);

    Constructor<?> sinkCtor = sinkClass.getConstructor(int.class);
    Object sink = sinkCtor.newInstance(1024);
    Method wrapForOutput = compressionClass.getMethod("wrapForOutput", sinkClass, byte.class);

    try {
      try (OutputStream out = (OutputStream) wrapForOutput.invoke(compression, sink, MAGIC_V2)) {
        out.write(payload);
      }
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof UnsatisfiedLinkError) {
        fail(
            "Kafka '"
                + codec
                + "' compression hit UnsatisfiedLinkError in the shaded jar: the codec's Java class is"
                + " relocated under io.acryl.shaded while its native library exports symbols for the"
                + " canonical package. "
                + cause.getMessage());
      }
      if (cause instanceof NoClassDefFoundError) {
        fail(
            "Kafka '"
                + codec
                + "' compression could not resolve its codec class: a bundled copy was removed while"
                + " the shaded Kafka client still references the relocated name. "
                + cause.getMessage());
      }
      throw e;
    }

    int written = (int) sink.getClass().getMethod("position").invoke(sink);
    assertTrue(written > 0, codec + " compression produced no output");

    // Round-trip rather than stopping at "bytes were written": that only proves nothing threw, and
    // a
    // codec that produced an unreadable frame would still pass. Decompressing back to the original
    // payload asserts the native path actually works.
    ByteBuffer compressed = (ByteBuffer) sink.getClass().getMethod("buffer").invoke(sink);
    compressed.flip();
    Class<?> bufferSupplier = Class.forName(BUFFER_SUPPLIER);
    Object noCaching = bufferSupplier.getField("NO_CACHING").get(null);
    Method wrapForInput =
        compressionClass.getMethod("wrapForInput", ByteBuffer.class, byte.class, bufferSupplier);

    byte[] roundTripped;
    try (InputStream in =
        (InputStream) wrapForInput.invoke(compression, compressed, MAGIC_V2, noCaching)) {
      roundTripped = readAll(in);
    }
    assertArrayEquals(
        payload, roundTripped, codec + " decompressed to different bytes than were compressed");
  }

  private static byte[] readAll(InputStream in) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[512];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  /**
   * JNI-backed libraries must never be relocated, and the agent should not ship its own copies at
   * all — the host classpath provides them, and a relocated copy can only shadow the working one.
   * The OpenLineage SQL parser is the sole exception and stays at its canonical {@code
   * io/openlineage/sql/} path (issue #18558).
   */
  @Test
  public void agentDoesNotShipRelocatedJniClasses() {
    for (String relocated :
        new String[] {
          "io.acryl.shaded.org.xerial.snappy.SnappyNative",
          "io.acryl.shaded.com.github.luben.zstd.Zstd",
          "io.acryl.shaded.net.jpountz.lz4.LZ4JNI",
          "io.acryl.shaded.com.sun.jna.Native"
        }) {
      try {
        Class.forName(relocated);
        fail(
            relocated
                + " is still present in the shaded jar. JNI-backed libraries must not be relocated:"
                + " their native symbols are compiled under the canonical package name.");
      } catch (ClassNotFoundException expected) {
        // Correct — the canonical copy from the host classpath is used instead.
      }
    }
  }
}
