package io.datahubproject.openapi.operations.consistency.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixDetail;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

/** Details of a single fix operation. */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Details of a single fix operation")
public class FixDetail {

  /** Creates a FixDetail from a service-layer FixDetail. */
  public static FixDetail from(@Nonnull ConsistencyFixDetail serviceDetail) {
    return FixDetail.builder()
        .urn(serviceDetail.getUrn().toString())
        .action(serviceDetail.getAction())
        .success(serviceDetail.isSuccess())
        .errorMessage(serviceDetail.getErrorMessage())
        .details(serviceDetail.getDetails())
        .build();
  }

  /** Creates a list of FixDetails from service-layer FixDetails. */
  public static List<FixDetail> from(@Nonnull List<ConsistencyFixDetail> serviceDetails) {
    return serviceDetails.stream().map(FixDetail::from).collect(Collectors.toList());
  }

  @Schema(description = "URN of the entity", example = "urn:li:assertion:abc123")
  @Nonnull
  private String urn;

  @Schema(description = "Action taken or that would be taken", enumAsRef = true)
  @Nonnull
  private ConsistencyFixType action;

  @Schema(description = "Whether the fix succeeded")
  private boolean success;

  @Schema(description = "Error message if fix failed")
  @Nullable
  private String errorMessage;

  @Schema(description = "Additional details about the fix")
  @Nullable
  private String details;
}
