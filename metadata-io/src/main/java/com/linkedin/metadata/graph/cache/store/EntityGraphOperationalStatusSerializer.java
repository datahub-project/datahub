package com.linkedin.metadata.graph.cache.store;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import java.io.IOException;
import javax.annotation.Nonnull;

public class EntityGraphOperationalStatusSerializer
    implements StreamSerializer<EntityGraphOperationalStatus> {

  @Override
  public int getTypeId() {
    return 9003;
  }

  @Override
  public void write(@Nonnull ObjectDataOutput out, @Nonnull EntityGraphOperationalStatus value)
      throws IOException {
    out.writeString(value.getStatus());
    Long recordedAt = value.getRecordedAtMillis();
    out.writeBoolean(recordedAt != null);
    if (recordedAt != null) {
      out.writeLong(recordedAt);
    }
  }

  @Override
  @Nonnull
  public EntityGraphOperationalStatus read(@Nonnull ObjectDataInput in) throws IOException {
    String status = in.readString();
    Long recordedAt = in.readBoolean() ? in.readLong() : null;
    return EntityGraphOperationalStatus.builder()
        .status(status)
        .recordedAtMillis(recordedAt)
        .build();
  }
}
