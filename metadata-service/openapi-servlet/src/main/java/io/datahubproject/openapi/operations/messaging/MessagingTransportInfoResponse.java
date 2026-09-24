package io.datahubproject.openapi.operations.messaging;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Active metadata messaging transport and primary topic names")
public class MessagingTransportInfoResponse {

  @Schema(description = "kafka or pgqueue")
  private String transport;

  private String metadataChangeProposalTopic;
  private String metadataChangeLogVersionedTopic;
  private String metadataChangeLogTimeseriesTopic;
}
