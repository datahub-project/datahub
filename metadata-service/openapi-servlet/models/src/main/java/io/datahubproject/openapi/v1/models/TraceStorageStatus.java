package io.datahubproject.openapi.v1.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.datahubproject.metadata.exception.TraceException;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
public class TraceStorageStatus {
  public static final TraceStorageStatus NO_OP = TraceStorageStatus.ok(TraceWriteStatus.NO_OP);

  public static TraceStorageStatus ok(TraceWriteStatus writeStatus) {
    return TraceStorageStatus.builder().writeStatus(writeStatus).build();
  }

  public static TraceStorageStatus ok(TraceWriteStatus writeStatus, @Nullable String message) {
    TraceStorageStatus.TraceStorageStatusBuilder builder =
        TraceStorageStatus.builder().writeStatus(writeStatus);
    if (message != null) {
      builder.writeMessage(message);
    }
    return builder.build();
  }

  public static TraceStorageStatus fail(TraceWriteStatus writeStatus, @Nullable Throwable t) {
    TraceStorageStatus.TraceStorageStatusBuilder builder =
        TraceStorageStatus.builder().writeStatus(writeStatus);
    if (t != null) {
      builder.writeExceptions(List.of(new TraceException(t)));
    }
    return builder.build();
  }

  public static TraceStorageStatus fail(TraceWriteStatus writeStatus, @Nullable String message) {
    TraceStorageStatus.TraceStorageStatusBuilder builder =
        TraceStorageStatus.builder().writeStatus(writeStatus);
    if (message != null) {
      builder.writeMessage(message);
    }
    return builder.build();
  }

  private TraceWriteStatus writeStatus;
  private String writeMessage;
  @Nullable private List<TraceException> writeExceptions;
}
