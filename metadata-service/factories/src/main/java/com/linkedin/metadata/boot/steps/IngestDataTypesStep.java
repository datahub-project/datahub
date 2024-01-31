package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datatype.DataTypeInfo;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

/** This bootstrap step is responsible for ingesting default data types. */
@Slf4j
public class IngestDataTypesStep implements BootstrapStep {

  private static final String DEFAULT_FILE_PATH = "./boot/data_types.json";
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private final EntityService<?> _entityService;
  private final String _resourcePath;

  public IngestDataTypesStep(@Nonnull final EntityService<?> entityService) {
    this(entityService, DEFAULT_FILE_PATH);
  }

  public IngestDataTypesStep(
      @Nonnull final EntityService<?> entityService, @Nonnull final String filePath) {
    _entityService = Objects.requireNonNull(entityService, "entityService must not be null");
    _resourcePath = filePath;
  }

  @Override
  public String name() {
    return "IngestDataTypesStep";
  }

  @Override
  public void execute() throws Exception {
    log.info("Ingesting default data types...");

    // 1. Read from the file into JSON.
    final JsonNode dataTypesObj =
        JSON_MAPPER.readTree(new ClassPathResource(_resourcePath).getFile());

    if (!dataTypesObj.isArray()) {
      throw new RuntimeException(
          String.format(
              "Found malformed data types file, expected an Array but found %s",
              dataTypesObj.getNodeType()));
    }

    log.info("Ingesting {} data types types", dataTypesObj.size());
    int numIngested = 0;
    for (final JsonNode roleObj : dataTypesObj) {
      final Urn urn = Urn.createFromString(roleObj.get("urn").asText());
      final DataTypeInfo info =
          RecordUtils.toRecordTemplate(DataTypeInfo.class, roleObj.get("info").toString());
      log.info(String.format("Ingesting default data type with urn %s", urn));
      ingestDataType(urn, info);
      numIngested++;
    }
    log.info("Ingested {} new data types", numIngested);
  }

  private void ingestDataType(final Urn dataTypeUrn, final DataTypeInfo info) throws Exception {
    // Write key
    final MetadataChangeProposal keyAspectProposal = new MetadataChangeProposal();
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(dataTypeUrn.getEntityType());
    GenericAspect keyAspect =
        GenericRecordUtils.serializeAspect(
            EntityKeyUtils.convertUrnToEntityKey(dataTypeUrn, keyAspectSpec));
    keyAspectProposal.setAspect(keyAspect);
    keyAspectProposal.setAspectName(keyAspectSpec.getName());
    keyAspectProposal.setEntityType(DATA_TYPE_ENTITY_NAME);
    keyAspectProposal.setChangeType(ChangeType.UPSERT);
    keyAspectProposal.setEntityUrn(dataTypeUrn);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(dataTypeUrn);
    proposal.setEntityType(DATA_TYPE_ENTITY_NAME);
    proposal.setAspectName(DATA_TYPE_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(
        proposal,
        new AuditStamp()
            .setActor(Urn.createFromString(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis()),
        false);
  }
}
