package datahub.client.file;

import datahub.event.EventFormatter;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class FileEmitterConfig {
  @Builder.Default
  @lombok.NonNull
  private final String fileName = null;
  @Builder.Default
  private final EventFormatter eventFormatter = new EventFormatter(EventFormatter.Format.PEGASUS_JSON);
  
}
