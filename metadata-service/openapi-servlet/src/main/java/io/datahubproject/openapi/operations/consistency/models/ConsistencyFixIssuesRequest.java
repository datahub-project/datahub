package io.datahubproject.openapi.operations.consistency.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Request to fix specific consistency issues. */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Request to fix specific consistency issues")
public class ConsistencyFixIssuesRequest {

  @Schema(
      description = "Issues to fix (from /check response). Each issue contains its own entityType.",
      required = true)
  @Nonnull
  private List<ConsistencyIssue> issues;

  @Schema(
      description =
          "If true, only report what would be fixed without making changes. "
              + "Set to false to apply actual fixes.",
      defaultValue = "true")
  private boolean dryRun = true;

  @Schema(
      description =
          "If true (default), writes are async for better performance. "
              + "Set to false for synchronous writes that wait for completion.",
      defaultValue = "true")
  private boolean async = true;
}
