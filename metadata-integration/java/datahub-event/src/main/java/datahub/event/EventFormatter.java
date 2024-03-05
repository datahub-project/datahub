package datahub.event;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import lombok.SneakyThrows;

/** A class that helps to format Metadata events for transport */
public class EventFormatter {

  private final ObjectMapper objectMapper;

  private final JacksonDataTemplateCodec dataTemplateCodec;
  private final Format serializationFormat;

  public EventFormatter(Format serializationFormat) {
    this.serializationFormat = serializationFormat;
    objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    dataTemplateCodec = new JacksonDataTemplateCodec(objectMapper.getFactory());
  }

  public EventFormatter() {
    this(Format.PEGASUS_JSON);
  }

  @SneakyThrows(URISyntaxException.class)
  public MetadataChangeProposal convert(MetadataChangeProposalWrapper mcpw) throws IOException {

    String serializedAspect =
        StringEscapeUtils.escapeJava(dataTemplateCodec.dataTemplateToString(mcpw.getAspect()));
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityType(mcpw.getEntityType())
            .setAspectName(mcpw.getAspectName())
            .setEntityUrn(Urn.createFromString(mcpw.getEntityUrn()))
            .setChangeType(mcpw.getChangeType());

    switch (this.serializationFormat) {
      case PEGASUS_JSON:
        {
          mcp.setAspect(
              new GenericAspect()
                  .setContentType("application/json")
                  .setValue(
                      ByteString.unsafeWrap(serializedAspect.getBytes(StandardCharsets.UTF_8))));
        }
        break;
      default:
        throw new EventValidationException(
            "Cannot handle serialization format " + this.serializationFormat);
    }
    return mcp;
  }

  public enum Format {
    PEGASUS_JSON,
  }
}
