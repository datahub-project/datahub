package com.linkedin.metadata.queue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

public class QueueHeadersJsonTest {

  @Test
  public void testSerializeEmptyReturnsNull() {
    assertNull(QueueHeadersJson.serialize(List.of()));
  }

  @Test
  public void testRoundTripSingleHeader() throws SQLException {
    byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
    List<QueueMessageHeader> headers = List.of(new QueueMessageHeader("correlationId", value));

    String json = QueueHeadersJson.serialize(headers);
    assertNotNull(json);
    assertTrue(json.contains("correlationId"));

    List<QueueMessageHeader> deserialized = QueueHeadersJson.deserialize(json);
    assertEquals(deserialized.size(), 1);
    assertEquals(deserialized.get(0).key(), "correlationId");
    assertTrue(Arrays.equals(deserialized.get(0).value(), value));
  }

  @Test
  public void testRoundTripMultipleHeaders() throws SQLException {
    List<QueueMessageHeader> headers =
        List.of(
            new QueueMessageHeader("key1", new byte[] {1, 2, 3}),
            new QueueMessageHeader("key2", new byte[] {4, 5}));

    String json = QueueHeadersJson.serialize(headers);
    List<QueueMessageHeader> deserialized = QueueHeadersJson.deserialize(json);

    assertEquals(deserialized.size(), 2);
    assertEquals(deserialized.get(0).key(), "key1");
    assertTrue(Arrays.equals(deserialized.get(0).value(), new byte[] {1, 2, 3}));
    assertEquals(deserialized.get(1).key(), "key2");
    assertTrue(Arrays.equals(deserialized.get(1).value(), new byte[] {4, 5}));
  }

  @Test
  public void testRoundTripBinaryValues() throws SQLException {
    byte[] binary = new byte[256];
    for (int i = 0; i < 256; i++) {
      binary[i] = (byte) i;
    }
    List<QueueMessageHeader> headers = List.of(new QueueMessageHeader("bin", binary));

    String json = QueueHeadersJson.serialize(headers);
    List<QueueMessageHeader> deserialized = QueueHeadersJson.deserialize(json);

    assertEquals(deserialized.size(), 1);
    assertTrue(Arrays.equals(deserialized.get(0).value(), binary));
  }

  @Test
  public void testDeserializeNullReturnsEmpty() throws SQLException {
    assertTrue(QueueHeadersJson.deserialize(null).isEmpty());
  }

  @Test
  public void testDeserializeBlankStringReturnsEmpty() throws SQLException {
    assertTrue(QueueHeadersJson.deserialize("").isEmpty());
    assertTrue(QueueHeadersJson.deserialize("  ").isEmpty());
  }

  @Test
  public void testDeserializeNonArrayReturnsEmpty() throws SQLException {
    assertTrue(QueueHeadersJson.deserialize("{\"not\":\"array\"}").isEmpty());
  }

  @Test(expectedExceptions = SQLException.class)
  public void testDeserializeMalformedJsonThrowsSqlException() throws SQLException {
    QueueHeadersJson.deserialize("not valid json {{");
  }

  @Test
  public void testDeserializeNonStringObjectCallsToString() throws SQLException {
    Object pgJsonb =
        new Object() {
          @Override
          public String toString() {
            return "[{\"key\":\"k\",\"v\":\"aGk=\"}]";
          }
        };
    List<QueueMessageHeader> result = QueueHeadersJson.deserialize(pgJsonb);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).key(), "k");
    assertTrue(Arrays.equals(result.get(0).value(), "hi".getBytes(StandardCharsets.UTF_8)));
  }
}
