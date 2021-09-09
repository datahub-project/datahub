package com.linkedin.metadata.boot.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.search.utils.BrowsePathUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

import javax.xml.crypto.Data;


@Slf4j
public class IngestDataPlatformsStep implements BootstrapStep {

  private static final String PLATFORM_ASPECT_NAME = "dataPlatformInfo";

  private final EntityService _entityService;

  public IngestDataPlatformsStep(final EntityService entityService) {
    _entityService = entityService;
  }

  @Override
  public String name() {
    return "IngestDataPlatformsStep";
  }

  @Override
  public void execute() throws IOException, URISyntaxException {

    final ObjectMapper mapper = new ObjectMapper();

    // 1. Read from the file into JSON.
    final JsonNode dataPlatforms = mapper.readTree(new ClassPathResource("./boot/data_platforms.json").getFile());

    if (!dataPlatforms.isArray()) {
      throw new RuntimeException(String.format("Found malformed data platforms file, expected an Array but found %s",
          dataPlatforms.getNodeType()));
    }

    // 2. For each JSON object, cast into a DataPlatformSnapshot object.
    for (final JsonNode dataPlatform : dataPlatforms) {
      final Urn urn;
      try {
        urn = Urn.createFromString(dataPlatform.get("urn").asText());
      } catch (URISyntaxException e) {
        log.error("Malformed urn: {}", dataPlatform.get("urn").asText());
        throw new RuntimeException("Malformed urn", e);
      }
      final DataPlatformInfo info =
          RecordUtils.toRecordTemplate(DataPlatformInfo.class, dataPlatform.get("aspect").toString());

      final AuditStamp aspectAuditStamp =
          new AuditStamp().setActor(Urn.createFromString(Constants.UNKNOWN_ACTOR)).setTime(System.currentTimeMillis());

      _entityService.ingestAspect(urn, PLATFORM_ASPECT_NAME, info, aspectAuditStamp);
    }
  }
}
