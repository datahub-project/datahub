/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.event.models.v1;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Schema(description = "The response provided when fetching external events")
@Data
@Getter
@Setter
public class ExternalEventsResponse {
  @Schema(description = "Offset id the stream for scrolling", example = "0")
  private String offsetId;

  @Schema(description = "Count of the events", example = "100")
  private Long count;

  @Schema(description = "The raw events")
  private List<ExternalEvent> events;

  @Schema(description = "The error message if the request failed")
  private String errorMessage;
}
