package datahub.client;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class MetadataWriteResponse {

  /** True if the client send succeeded and we got a successful response from the server */
  @Builder.Default boolean success = true;

  /**
   * If the write failed due to an exception thrown by the server and we have access to it, then we
   * store the stack trace here
   */
  String responseContent;

  /** The underlying response object (typically an HTTPResponse or a kafka.ResponseMetadata) */
  Object underlyingResponse;
}
