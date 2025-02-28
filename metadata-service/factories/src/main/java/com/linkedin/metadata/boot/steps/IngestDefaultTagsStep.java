package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.tag.TagProperties;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

@Slf4j
public class IngestDefaultTagsStep implements BootstrapStep {

  private static final String DEFAULT_FILE_PATH = "./boot/tags.json";
  private final EntityService<?> _entityService;
  private final String _filePath;

  public IngestDefaultTagsStep(@Nonnull final EntityService<?> entityService) {
    this(entityService, DEFAULT_FILE_PATH);
  }

  public IngestDefaultTagsStep(
      @Nonnull final EntityService<?> entityService, @Nonnull final String filePath) {
    _entityService = entityService;
    _filePath = filePath;
  }

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void execute(@Nonnull OperationContext systemOperationContext)
      throws IOException, URISyntaxException {

    final ObjectMapper mapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    mapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());

    try {

      // 1. Read from the file into JSON.
      final JsonNode tags = mapper.readTree(new ClassPathResource(_filePath).getInputStream());

      if (!tags.isArray()) {
        throw new RuntimeException(
            String.format(
                "Found malformed tags file, expected an Array but found %s", tags.getNodeType()));
      }

      // 2. For each JSON object, cast into a Tag Properties object.
      for (final JsonNode tag : tags) {
        final String urnString;
        final Urn urn;
        try {
          urnString = tag.get("urn").asText();
          urn = Urn.createFromString(urnString);
        } catch (URISyntaxException e) {
          log.error("Malformed urn: {}", tag.get("urn").asText());
          throw new RuntimeException("Malformed urn", e);
        }

        final TagProperties tagProperties =
            (TagProperties)
                _entityService.getLatestAspect(
                    systemOperationContext, urn, Constants.TAG_PROPERTIES_ASPECT_NAME);
        // Skip ingesting for this JSON object if info already exists.
        if (tagProperties != null) {
          log.debug(
              String.format(
                  "%s already exists for %s. Skipping...",
                  Constants.TAG_PROPERTIES_ASPECT_NAME, urnString));
          continue;
        }

        final TagProperties properties =
            RecordUtils.toRecordTemplate(TagProperties.class, tag.get("properties").toString());

        final AuditStamp aspectAuditStamp =
            new AuditStamp()
                .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                .setTime(System.currentTimeMillis());

        _entityService.ingestProposal(
            systemOperationContext,
            AspectUtils.buildMetadataChangeProposal(
                urn, Constants.TAG_PROPERTIES_ASPECT_NAME, properties),
            aspectAuditStamp,
            false);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to ingest default tags! Aborting startup...", e);
    }
  }
}
