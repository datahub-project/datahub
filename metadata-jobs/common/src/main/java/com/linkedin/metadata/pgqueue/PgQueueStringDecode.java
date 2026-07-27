package com.linkedin.metadata.pgqueue;

import com.linkedin.metadata.queue.PgQueuePayloadCodec;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

/** Decode UTF-8 text payloads from pgQueue (CDC, usage events). */
public final class PgQueueStringDecode {

  private PgQueueStringDecode() {}

  @Nonnull
  public static String decodeAsUtf8(@Nonnull QueueReceivedMessage message) {
    byte[] raw = PgQueuePayloadCodec.decode(message.payload(), message.payloadCompression());
    return new String(raw, StandardCharsets.UTF_8);
  }
}
