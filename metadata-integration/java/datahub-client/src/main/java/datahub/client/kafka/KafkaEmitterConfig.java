package datahub.client.kafka;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import datahub.event.EventFormatter;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Slf4j
public class KafkaEmitterConfig {

  public static final String CLIENT_VERSION_PROPERTY = "clientVersion";

  @Builder.Default
  private final String bootstrap = "localhost:9092";
  @Builder.Default
  private final String schemaRegistryUrl = "http://localhost:8081";

  private final Map<String, String> schemaRegistryConfig = Collections.emptyMap();
  private final Map<String, String> producerConfig = Collections.emptyMap();

  @Builder.Default
  private final EventFormatter eventFormatter = new EventFormatter(EventFormatter.Format.PEGASUS_JSON);
  
  public static class KafkaEmitterConfigBuilder {

    @SuppressWarnings("unused")
    private String getVersion() {
      try (InputStream foo = this.getClass().getClassLoader().getResourceAsStream("client.properties")) {
        Properties properties = new Properties();
        properties.load(foo);
        return properties.getProperty(CLIENT_VERSION_PROPERTY, "unknown");
      } catch (Exception e) {
        log.warn("Unable to find a version for datahub-client. Will set to unknown", e);
        return "unknown";
      }
    }

    public KafkaEmitterConfigBuilder with(Consumer<KafkaEmitterConfigBuilder> builderFunction) {
      builderFunction.accept(this);
      return this;
    }

  }

}
