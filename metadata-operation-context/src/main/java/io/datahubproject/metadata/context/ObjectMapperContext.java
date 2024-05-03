package io.datahubproject.metadata.context;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.Constants;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ObjectMapperContext implements ContextInterface {

  public static ObjectMapper defaultMapper = new ObjectMapper();

  static {
    defaultMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(
                    Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH,
                    Constants.MAX_JACKSON_STRING_SIZE));
    defaultMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  public static ObjectMapperContext DEFAULT = ObjectMapperContext.builder().build();

  @Nonnull private final ObjectMapper objectMapper;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }

  public static class ObjectMapperContextBuilder {
    public ObjectMapperContext build() {
      if (this.objectMapper == null) {
        objectMapper(defaultMapper);
      }
      return new ObjectMapperContext(this.objectMapper);
    }
  }
}
